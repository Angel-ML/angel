package com.tencent.angel.ml.lda;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.lda.get.PartCSRResult;
import com.tencent.angel.ml.lda.structures.*;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.special.Gamma;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;

import java.util.Random;

public class Sampler {

  CSRTokens data;
  LDAModel model;

  FTree tree;
  float[] psum;
  short[] tidx;
  int[] nk;
  int[] wk;

  int K;
  float alpha, beta, vbeta;
  double lgammaAlpha, lgammaBeta;
  double lgammaAlphaSum;

  boolean error = false;

  private final static Log LOG = LogFactory.getLog(Sampler.class);


  public Sampler(CSRTokens data, LDAModel model) {
    this.data = data;
    this.model = model;

    K = model.K();
    alpha = model.alpha();
    beta  = model.beta();
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

  public void sample(PartitionKey pkey, PartCSRResult csr) {
    int ws = pkey.getStartRow();
    int we = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    for (int w = ws; w < we; w ++) {
      if (!csr.read(wk)) throw new AngelException("some error happens");

      buildFTree();
      DenseIntVector update = new DenseIntVector(K);

      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi ++) {
        int d = data.docs[wi];
        TraverseHashMap dk = data.dks[d];
        int tt = data.topics[wi];

        if (wk[tt] <= 0) {
          LOG.error(String.format("Error wk[%d] = %d for word %d", tt, wk[tt], w));
          error = true;
          continue;
        }

        wk[tt] --;
        nk[tt] --;
        float value = (wk[tt] + beta) / (nk[tt] + vbeta);
        tree.update(tt, value);
        update.plusBy(tt, -1);

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

        wk[tt] ++;
        nk[tt] ++;
        value = (wk[tt] + beta) / (nk[tt] + vbeta);
        tree.update(tt, value);
        data.topics[wi] = tt;
        update.plusBy(tt, 1);
      }

      model.wtMat().increment(w, update);
    }
  }

  private void buildFTree() {
    for (int k = 0; k < K; k ++)
      psum[k] = (wk[k] + beta) / (nk[k] + vbeta);
    tree.build(psum);
  }

  private float build(S2STraverseMap dk) {
    float sum = 0;
    for (int i = 0; i < dk.size; i ++) {
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
    for (int i = 0; i < dk.size; i ++) {
      short k = dk.key[dk.idx[i]];
      short v = dk.value[dk.idx[i]];
      sum += v * tree.get(k);
      psum[i] = sum;
      tidx[i] = k;
    }
    return sum;
  }

  private float build(TraverseHashMap dk) {
    if (dk instanceof S2STraverseMap)
      return build((S2STraverseMap) dk);
    else
      return build((S2BTraverseMap) dk);
  }

  public void initialize(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    for (int w = ws; w < es; w ++) {
      DenseIntVector update = new DenseIntVector(K);

      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi ++) {
        int d = data.docs[wi];
        int t = rand.nextInt(K);
        data.topics[wi] = t;
        nk[t] ++;
        synchronized (data.dks[d]) {
          data.dks[d].inc(t);
        }
        update.plusBy(t, 1);
      }

      model.wtMat().increment(w, update);
    }
  }

  public Sampler set(int[] nk) {
    System.arraycopy(nk, 0, this.nk, 0, K);
    return this;
  }

  public void reset(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    for (int w = ws; w < es; w ++) {
      DenseIntVector update = new DenseIntVector(K);
      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi ++) {
        int tt = data.topics[wi];
        update.plusBy(tt, 1);
        nk[tt] ++;
      }
      model.wtMat().increment(w, update);
    }
  }

  public void initForInference(PartitionKey pkey) {
    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    Random rand = new Random(System.currentTimeMillis());

    for (int w = ws; w < es; w ++) {
      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi ++) {
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

    for (int w = ws; w < we; w ++) {
      if (!csr.read(wk)) throw new AngelException("some error happens");

      buildFTree();

      for (int wi = data.ws[w]; wi < data.ws[w + 1]; wi ++) {
        int d = data.docs[wi];
        TraverseHashMap dk = data.dks[d];
        int tt = data.topics[wi];

        if (wk[tt] <= 0) {
          LOG.error(String.format("Error wk[%d] = %d for word %d", tt, wk[tt], w));
          error = true;
          continue;
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
