/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
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
import com.tencent.angel.ml.lda.psf.CSRPartUpdateParam;
import com.tencent.angel.ml.lda.psf.PartCSRResult;
import com.tencent.angel.ml.lda.psf.UpdatePartFunc;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import it.unimi.dsi.fastutil.shorts.Short2IntMap;
import it.unimi.dsi.fastutil.shorts.Short2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.special.Gamma;

import com.tencent.angel.ml.lda.algo.structures.*;

import java.util.Random;
import java.util.concurrent.Future;

public class Sampler {

  public CSRTokens data;
  public LDAModel model;

  public FTree tree;
  public float[] psum;
  public short[] tidx;
  public int[] nk;
  public int[] wk;

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
    tidx = new short[K];
    psum = new float[K];

    tree = new FTree(K);
  }

  public Future<VoidResult> sample(PartitionKey pkey, PartCSRResult csr) {
    int ws = pkey.getStartRow();
    int we = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    Short2IntOpenHashMap[] updates = new Short2IntOpenHashMap[we - ws];

    for (int w = ws; w < we; w++) {

      if (data.ws[w + 1] - data.ws[w] == 0)
        continue;

      if (!csr.read(wk))
        throw new AngelException("some error happens");

      buildFTree();
      updates[w - ws] = new Short2IntOpenHashMap();
      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int d = data.docs[wi];
        TraverseHashMap dk = data.dks[d];
        int tt = data.topics[wi];

        if (wk[tt] <= 0) {
          LOG.error(String.format("Error wk[%d] = %d for word %d", tt, wk[tt], w));
          continue;
        }

        wk[tt]--;
        nk[tt]--;
        float value = (wk[tt] + beta) / (nk[tt] + vbeta);
        tree.update(tt, value);
        updates[w - ws].addTo((short) tt, -1);

        synchronized (dk) {
          dk.dec(tt);
          float sum = build(dk);
          float u = rand.nextFloat() * (sum + alpha * tree.first());
          if (u < sum) {
            u = rand.nextFloat() * sum;
            int idx = BinarySearch.binarySearch(psum, u, 0, dk.size - 1);
            tt = tidx[idx];
          } else
            tt = tree.sample(rand.nextFloat() * tree.first());

          dk.inc(tt);
        }

        wk[tt]++;
        nk[tt]++;
        value = (wk[tt] + beta) / (nk[tt] + vbeta);
        tree.update(tt, value);
        data.topics[wi] = tt;
        updates[w - ws].addTo((short) tt, 1);
      }

      //      model.wtMat().increment(w, update);
    }
    CSRPartUpdateParam param = new CSRPartUpdateParam(model.wtMat().getMatrixId(), pkey, updates);
    Future<VoidResult> future =
      PSAgentContext.get().getMatrixTransportClient().update(new UpdatePartFunc(null), param);
    return future;
  }

  private void buildFTree() {
    for (int k = 0; k < K; k++)
      psum[k] = (wk[k] + beta) / (nk[k] + vbeta);
    tree.build(psum);
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

  private float build(TraverseHashMap dk) {
    if (dk instanceof S2STraverseMap)
      return build((S2STraverseMap) dk);
    if (dk instanceof S2BTraverseMap)
      return build((S2BTraverseMap) dk);
    if (dk instanceof S2ITraverseMap)
      return build((S2ITraverseMap) dk);
    return 0.0F;
  }

  public Future<VoidResult> initialize(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    Short2IntOpenHashMap[] updates = new Short2IntOpenHashMap[es - ws];

    for (int w = ws; w < es; w++) {
      if (data.ws[w + 1] == data.ws[w])
        continue;

      updates[w - ws] = new Short2IntOpenHashMap();

      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int d = data.docs[wi];
        int t = rand.nextInt(K);
        data.topics[wi] = t;
        nk[t]++;
        synchronized (data.dks[d]) {
          data.dks[d].inc(t);
        }
        //        update.plusBy(t, 1);
        updates[w - ws].addTo((short) t, 1);
      }

      //      model.wtMat().increment(w, update);
    }

    CSRPartUpdateParam param = new CSRPartUpdateParam(model.wtMat().getMatrixId(), pkey, updates);
    Future<VoidResult> future =
      PSAgentContext.get().getMatrixTransportClient().update(new UpdatePartFunc(null), param);
    return future;
  }

  public Sampler set(int[] nk) {
    System.arraycopy(nk, 0, this.nk, 0, K);
    return this;
  }

  public Future<VoidResult> reset(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();
    Short2IntOpenHashMap[] updates = new Short2IntOpenHashMap[es - ws];

    for (int w = ws; w < es; w++) {
      if (data.ws[w + 1] == data.ws[w])
        continue;

      updates[w - ws] = new Short2IntOpenHashMap();
      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int tt = data.topics[wi];
        updates[w - ws].addTo((short) tt, 1);
        nk[tt]++;
      }
    }

    CSRPartUpdateParam param = new CSRPartUpdateParam(model.wtMat().getMatrixId(), pkey, updates);
    Future<VoidResult> future =
      PSAgentContext.get().getMatrixTransportClient().update(new UpdatePartFunc(null), param);
    return future;
  }

  public void initForInference(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    for (int w = ws; w < es; w++) {
      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int d = data.docs[wi];
        int t = rand.nextInt(K);
        data.topics[wi] = t;
        synchronized (data.dks[d]) {
          data.dks[d].inc(t);
        }
      }
    }
  }

  public void inference(PartitionKey pkey, PartCSRResult csr) {
    int ws = pkey.getStartRow();
    int we = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    for (int w = ws; w < we; w++) {

      if (data.ws[w + 1] - data.ws[w] == 0)
        continue;

      if (!csr.read(wk))
        throw new AngelException("some error happens");

      buildFTree();

      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi++) {
        int d = data.docs[wi];
        TraverseHashMap dk = data.dks[d];
        int tt = data.topics[wi];

        if (wk[tt] < 0) {
          LOG.error(String.format("Error wk[%d] = %d for word %d", tt, wk[tt], w));
          throw new AngelException("some error happens");
        }

        synchronized (dk) {
          dk.dec(tt);
          float sum = build(dk);
          float u = rand.nextFloat() * (sum + alpha * tree.first());
          if (u < sum) {
            u = rand.nextFloat() * sum;
            int idx = BinarySearch.binarySearch(psum, u, 0, dk.size - 1);
            tt = tidx[idx];
          } else
            tt = tree.sample(rand.nextFloat() * tree.first());

          dk.inc(tt);
        }

        data.topics[wi] = tt;
      }
    }
  }

}
