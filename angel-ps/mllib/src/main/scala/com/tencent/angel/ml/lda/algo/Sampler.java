/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.lda.algo;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.lda.LDAModel;
import com.tencent.angel.ml.lda.algo.structures.*;
import com.tencent.angel.ml.lda.psf.CSRPartUpdateParam;
import com.tencent.angel.ml.lda.psf.PartCSRResult;
import com.tencent.angel.ml.lda.psf.UpdatePartFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.special.Gamma;

import java.util.Random;
import java.util.concurrent.Future;

public class Sampler {

  public CSRTokens data;
  public LDAModel model;

  public FTree tree;
  public float[] psum;
  public int[] tidx;
  public int[] nk;
  public int[] wk;
  public float[] maxDoc;

  public int K;
  public float alpha, beta, vbeta;
  public double lgammaAlpha, lgammaBeta;
  public double lgammaAlphaSum;

  public boolean error = false;

  private final static Log LOG = LogFactory.getLog(Sampler.class);


  public Sampler(CSRTokens data, LDAModel model) {
    this.data = data;
    this.model = model;

    K = model.K();
    alpha = model.alpha();
    beta = model.beta();
    vbeta = data.n_words * beta;

    lgammaBeta = Gamma.logGamma(beta);
    lgammaAlpha = Gamma.logGamma(alpha);
    lgammaAlphaSum = Gamma.logGamma(alpha * K);

    nk = new int[K];
    wk = new int[K];
    tidx = new int[K];
    psum = new float[K];
    maxDoc = new float[Math.min(data.maxDocLen, K)];

    tree = new FTree(K);
  }

  public Future<VoidResult> sample(PartitionKey pkey, PartCSRResult csr) {
    return sample(pkey, csr, true);
  }

  public Future<VoidResult> sample(PartitionKey pkey, PartCSRResult csr, boolean update) {
    int ws = pkey.getStartRow();
    int we = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());
    Int2IntOpenHashMap[] updates = null;
    try {
      // allocate update maps
      if (update)
        updates = new Int2IntOpenHashMap[we - ws];

      float sum, u;
      int idx;

      for (int w = ws; w < we; w++) {

        // Skip if no token for this word
        if (data.ws[w + 1] - data.ws[w] == 0)
          continue;

        // Check whether error when fetching word-topic
        if (!csr.read(wk))
          throw new AngelException("some error happens");

        // Build FTree for current word
        buildFTree();

        if (update)
          updates[w - ws] = new Int2IntOpenHashMap();

        for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
          // current doc
          int d = data.docs[wi];
          // old topic assignment
          int tt = data.topics[data.dindex[wi]];

          // Check if error happens. if this happen, it's probably that failures happen to servers.
          // We need to adjust the memory settings or network fetching parameters.
          if (update && wk[tt] <= 0) {
            LOG.error(String.format("Error wk[%d] = %d for word %d", tt, wk[tt], w));
            continue;
          }

          // Update statistics if needed
          if (update) {
            wk[tt]--;
            nk[tt]--;
            tree.update(tt, (wk[tt] + beta) / (nk[tt] + vbeta));
            updates[w - ws].addTo(tt, -1);
          }

          // Calculate psum and sample new topic
          synchronized (data.docIds[d]) {

            if (data.dks[d] == null)
              sum = build(d, maxDoc, tree, tt);
            else {
              data.dks[d].dec(tt);
              sum = build(data.dks[d]);
            }

            u = rand.nextFloat() * (sum + alpha * tree.first());

            if (u < sum) {
              u = rand.nextFloat() * sum;
              if (data.dks[d] == null) {
                int length = data.ds[d + 1] - data.ds[d];
                idx = BinarySearch.binarySearch(maxDoc, u, 0, length - 1);
                tt = data.topics[data.ds[d] + idx];
              } else {
                idx = BinarySearch.binarySearch(psum, u, 0, data.dks[d].size - 1);
                tt = tidx[idx];
              }
            } else
              tt = tree.sample(rand.nextFloat() * tree.first());

            if (data.dks[d] != null)
              data.dks[d].inc(tt);
          }

          // Update statistics if needed
          if (update) {
            wk[tt]++;
            nk[tt]++;
            tree.update(tt, (wk[tt] + beta) / (nk[tt] + vbeta));
            updates[w - ws].addTo(tt, 1);
          }

          // Assign new topic
          data.topics[data.dindex[wi]] = tt;
        }
      }
    } finally {
      csr.clear();
    }


    Future<VoidResult> future = null;
    if (update) {
      CSRPartUpdateParam param = new CSRPartUpdateParam(model.wtMat().getMatrixId(), pkey, updates);
      future =
        PSAgentContext.get().getMatrixTransportClient().update(new UpdatePartFunc(null), param);
    }
    return future;
  }

  private void buildFTree() {
    for (int k = 0; k < K; k++)
      psum[k] = (wk[k] + beta) / (nk[k] + vbeta);
    tree.build(psum);
  }

  public float build(int d, float[] p, FTree tree, int remove) {
    float psum = 0.0F;
    boolean find = false;
    for (int i = data.ds[d]; i < data.ds[d + 1]; i++) {
      int tt = data.topics[i];
      if (!find && tt == remove)
        find = true;
      else
        psum += tree.get(tt);
      p[i - data.ds[d]] = psum;
    }
    return psum;
  }

  private float build(S2STraverseMap dk) {
    float sum = 0;
    for (int i = 0; i < dk.size; i++) {
      short k = dk.key[dk.idx[i]];
      short v = dk.value[dk.idx[i]];
      sum += v * tree.get(k);
      psum[i] = sum;
      tidx[i] = k;
    }
    return sum;
  }

  private float build(S2BTraverseMap dk) {
    float sum = 0;
    for (int i = 0; i < dk.size; i++) {
      short k = dk.key[dk.idx[i]];
      short v = dk.value[dk.idx[i]];
      sum += v * tree.get(k);
      psum[i] = sum;
      tidx[i] = k;
    }
    return sum;
  }

  private float build(S2ITraverseMap dk) {
    float sum = 0;
    for (int i = 0; i < dk.size; i++) {
      short k = dk.key[dk.idx[i]];
      int v = dk.value[dk.idx[i]];
      sum += v * tree.get(k);
      psum[i] = sum;
      tidx[i] = k;
    }
    return sum;
  }

  private float build(I2ITranverseMap dk) {
    float sum = 0;
    for (int i = 0; i < dk.size; i++) {
      int k = dk.key[dk.idx[i]];
      int v = dk.value[dk.idx[i]];
      sum += v * tree.get(k);
      psum[i] = sum;
      tidx[i] = k;
    }
    return sum;
  }

  private float build(TraverseHashMap dk) {
    if (dk instanceof S2STraverseMap)
      return build((S2STraverseMap) dk);
    if (dk instanceof S2BTraverseMap)
      return build((S2BTraverseMap) dk);
    if (dk instanceof S2ITraverseMap)
      return build((S2ITraverseMap) dk);
    if (dk instanceof I2ITranverseMap)
      return build((I2ITranverseMap) dk);
    return 0.0F;
  }

  public Future<VoidResult> initialize(PartitionKey pkey) {
    return initialize(pkey, true);
  }

  public Future<VoidResult> initialize(PartitionKey pkey, boolean update) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    Int2IntOpenHashMap[] updates = null;
    if (update)
      updates = new Int2IntOpenHashMap[es - ws];

    for (int w = ws; w < es; w++) {
      // Skip if no token for this word
      if (data.ws[w + 1] == data.ws[w])
        continue;

      if (update)
        updates[w - ws] = new Int2IntOpenHashMap();

      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int t = rand.nextInt(K);
        data.topics[data.dindex[wi]] = t;

        if (update) {
          updates[w - ws].addTo(t, 1);
          nk[t]++;
        }

        int d = data.docs[wi];
        if (data.dks[d] != null) {
          synchronized (data.dks[d]) {
            data.dks[d].inc(t);
          }
        }
      }
    }

    Future<VoidResult> future = null;
    if (update) {
      CSRPartUpdateParam param = new CSRPartUpdateParam(model.wtMat().getMatrixId(), pkey, updates);
      future =
        PSAgentContext.get().getMatrixTransportClient().update(new UpdatePartFunc(null), param);
    }
    return future;
  }

  public Sampler set(int[] nk) {
    System.arraycopy(nk, 0, this.nk, 0, K);
    return this;
  }

  public Future<VoidResult> reset(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();
    Int2IntOpenHashMap[] updates = new Int2IntOpenHashMap[es - ws];

    for (int w = ws; w < es; w++) {
      if (data.ws[w + 1] == data.ws[w])
        continue;

      updates[w - ws] = new Int2IntOpenHashMap();
      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int tt = data.topics[data.dindex[wi]];
        updates[w - ws].addTo(tt, 1);
        nk[tt]++;
      }
    }

    CSRPartUpdateParam param = new CSRPartUpdateParam(model.wtMat().getMatrixId(), pkey, updates);
    Future<VoidResult> future =
      PSAgentContext.get().getMatrixTransportClient().update(new UpdatePartFunc(null), param);
    return future;
  }

  public void initForInference(PartitionKey pkey) {
    initialize(pkey, false);
  }

  public void inference(PartitionKey pkey, PartCSRResult csr) {
    sample(pkey, csr, false);
  }

}
