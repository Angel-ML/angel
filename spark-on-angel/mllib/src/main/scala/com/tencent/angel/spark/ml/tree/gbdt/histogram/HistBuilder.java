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


package com.tencent.angel.spark.ml.tree.gbdt.histogram;

import com.google.common.base.Preconditions;
import com.tencent.angel.spark.ml.tree.data.DataSet;
import com.tencent.angel.spark.ml.tree.data.FeatureRow;
import com.tencent.angel.spark.ml.tree.data.InstanceRow;
import com.tencent.angel.spark.ml.tree.gbdt.metadata.DataInfo;
import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo;
import com.tencent.angel.spark.ml.tree.param.GBDTParam;
import com.tencent.angel.spark.ml.tree.util.Maths;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class HistBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(HistBuilder.class);

  private final GBDTParam param;

  private static final int MIN_INSTANCE_PER_THREAD = 10000;
  private ExecutorService threadPool;
  private FPBuilderThread[] fpThreads;
  private FPBuilderThread2[] fpThreads2;

  public HistBuilder(final GBDTParam param) {
    this.param = param;
    if (param.numThread > 1) {
      this.threadPool = Executors.newFixedThreadPool(param.numThread);
      this.fpThreads = new FPBuilderThread[param.numThread];
      for (int threadId = 0; threadId < param.numThread; threadId++) {
        this.fpThreads[threadId] = new FPBuilderThread(threadId, param);
        this.fpThreads2[threadId] = new FPBuilderThread2(threadId, param);
      }
    }
  }

  public void shutdown() {
    if (threadPool != null && !threadPool.isShutdown()) {
      threadPool.shutdown();
    }
  }

  private static class FPBuilderThread implements Callable<Histogram[]> {

    int threadId;
    GBDTParam param;
    boolean[] isFeatUsed;
    int featLo;
    FeatureInfo featureInfo;
    InstanceRow[] instanceRows;
    GradPair[] gradPairs;
    int[] nodeToIns;
    int from, to;

    public FPBuilderThread(int threadId, GBDTParam param) {
      this.threadId = threadId;
      this.param = param;
    }

    @Override
    public Histogram[] call() throws Exception {
      return sparseBuildFP(param, isFeatUsed, featLo, featureInfo, instanceRows,
          gradPairs, nodeToIns, from, to);
    }
  }

  private static Histogram[] sparseBuildFP(GBDTParam param, boolean[] isFeatUsed,
      int featLo, FeatureInfo featureInfo,
      InstanceRow[] instanceRows, GradPair[] gradPairs,
      int[] nodeToIns, int from, int to) {
    Histogram[] histograms = new Histogram[isFeatUsed.length];
    for (int i = 0; i < isFeatUsed.length; i++) {
      if (isFeatUsed[i]) {
        histograms[i] = new Histogram(featureInfo.getNumBin(featLo + i),
            param.numClass, param.fullHessian, param.isMultiClassMultiTree());
      }
    }
    for (int posId = from; posId < to; posId++) {
      int insId = nodeToIns[posId];
      instanceRows[insId].accumulate(histograms, gradPairs[insId], isFeatUsed, featLo);
//            InstanceRow ins = instanceRows[insId];
//            int[] indices = ins.indices();
//            int[] bins = ins.bins();
//            int nnz = indices.length;
//            for (int j = 0; j < nnz; j++) {
//                int fid = indices[j];
//                if (isFeatUsed[fid - featLo]) {
//                    histograms[fid - featLo].accumulate(bins[j], gradPairs[insId]);
//                }
//            }
    }
    return histograms;
  }

  private static class FPBuilderThread2 implements Callable<Histogram[]> {

    int threadId;
    GBDTParam param;
    boolean[] isFeatUsed;
    int featLo;
    FeatureInfo featureInfo;
    DataSet dataset;
    GradPair[] gradPairs;
    int[] nodeToIns;
    int from, to;

    public FPBuilderThread2(int threadId, GBDTParam param) {
      this.threadId = threadId;
      this.param = param;
    }

    @Override
    public Histogram[] call() throws Exception {
      return sparseBuildFP2(param, isFeatUsed, featLo, featureInfo, dataset,
          gradPairs, nodeToIns, from, to);
    }
  }

  private static Histogram[] sparseBuildFP2(GBDTParam param, boolean[] isFeatUsed,
      int featLo, FeatureInfo featureInfo,
      DataSet dataset, GradPair[] gradPairs,
      int[] nodeToIns, int from, int to) {
    Histogram[] histograms = new Histogram[isFeatUsed.length];
    for (int i = 0; i < isFeatUsed.length; i++) {
      if (isFeatUsed[i]) {
        histograms[i] = new Histogram(featureInfo.getNumBin(featLo + i),
            param.numClass, param.fullHessian, param.isMultiClassMultiTree());
      }
    }
    for (int posId = from; posId < to; posId++) {
      int insId = nodeToIns[posId];
      dataset.accumulate(histograms, insId, gradPairs[insId], isFeatUsed, featLo);

    }
    return histograms;
  }

  public Histogram[] buildHistogramsFP(boolean[] isFeatUsed, int featLo, InstanceRow[] instanceRows,
      FeatureInfo featureInfo, DataInfo dataInfo,
      int nid, GradPair sumGradPair) throws Exception {
    int nodeStart = dataInfo.getNodePosStart(nid);
    int nodeEnd = dataInfo.getNodePosEnd(nid);
    GradPair[] gradPairs = dataInfo.gradPairs();
    int[] nodeToIns = dataInfo.nodeToIns();

    Histogram[] res;
    if (param.numThread <= 1 || nodeEnd - nodeStart + 1 <= MIN_INSTANCE_PER_THREAD) {
      res = sparseBuildFP(param, isFeatUsed, featLo, featureInfo, instanceRows,
          gradPairs, nodeToIns, nodeStart, nodeEnd);
    } else {
      int actualNumThread = Math.min(param.numThread,
          Maths.idivCeil(nodeEnd - nodeStart + 1, MIN_INSTANCE_PER_THREAD));
      Future[] futures = new Future[actualNumThread];
      int avg = (nodeEnd - nodeStart + 1) / actualNumThread;
      int from = nodeStart, to = nodeStart + avg;
      for (int threadId = 0; threadId < actualNumThread; threadId++) {
        FPBuilderThread builder = fpThreads[threadId];
        builder.isFeatUsed = isFeatUsed;
        builder.featLo = featLo;
        builder.featureInfo = featureInfo;
        builder.instanceRows = instanceRows;
        builder.gradPairs = gradPairs;
        builder.nodeToIns = nodeToIns;
        builder.from = from;
        builder.to = to;
        from = to;
        to = Math.min(from + avg, nodeEnd + 1);
        futures[threadId] = threadPool.submit(builder);
      }
      res = (Histogram[]) futures[0].get();
      for (int threadId = 1; threadId < actualNumThread; threadId++) {
        Histogram[] hist = (Histogram[]) futures[threadId].get();
        for (int i = 0; i < res.length; i++) {
          if (res[i] != null) {
            res[i].plusBy(hist[i]);
          }
        }
      }
    }
    for (int i = 0; i < res.length; i++) {
      if (res[i] != null) {
        GradPair taken = res[i].sum();
        GradPair remain = sumGradPair.subtract(taken);
        int defaultBin = featureInfo.getDefaultBin(featLo + i);
        res[i].accumulate(defaultBin, remain);
      }
    }
    return res;
  }

  public Histogram[] buildHistogramsFP(boolean[] isFeatUsed, int featLo, DataSet dataset,
      FeatureInfo featureInfo, DataInfo dataInfo,
      int nid, GradPair sumGradPair, Histogram[] subtract) throws Exception {
    int nodeStart = dataInfo.getNodePosStart(nid);
    int nodeEnd = dataInfo.getNodePosEnd(nid);
    GradPair[] gradPairs = dataInfo.gradPairs();
    int[] nodeToIns = dataInfo.nodeToIns();

    Histogram[] res;
    if (param.numThread <= 1 || nodeEnd - nodeStart + 1 <= MIN_INSTANCE_PER_THREAD) {
      res = sparseBuildFP2(param, isFeatUsed, featLo, featureInfo, dataset,
          gradPairs, nodeToIns, nodeStart, nodeEnd);
    } else {
      int actualNumThread = Math.min(param.numThread,
          Maths.idivCeil(nodeEnd - nodeStart + 1, MIN_INSTANCE_PER_THREAD));
      Future[] futures = new Future[actualNumThread];
      int avg = (nodeEnd - nodeStart + 1) / actualNumThread;
      int from = nodeStart, to = nodeStart + avg;
      for (int threadId = 0; threadId < actualNumThread; threadId++) {
        FPBuilderThread2 builder = fpThreads2[threadId];
        builder.isFeatUsed = isFeatUsed;
        builder.featLo = featLo;
        builder.featureInfo = featureInfo;
        builder.dataset = dataset;
        builder.gradPairs = gradPairs;
        builder.nodeToIns = nodeToIns;
        builder.from = from;
        builder.to = to;
        from = to;
        to = Math.min(from + avg, nodeEnd + 1);
        futures[threadId] = threadPool.submit(builder);
      }
      res = (Histogram[]) futures[0].get();
      for (int threadId = 1; threadId < actualNumThread; threadId++) {
        Histogram[] hist = (Histogram[]) futures[threadId].get();
        for (int i = 0; i < res.length; i++) {
          if (res[i] != null) {
            res[i].plusBy(hist[i]);
          }
        }
      }
    }
    if (subtract == null) {
      for (int i = 0; i < res.length; i++) {
        if (res[i] != null) {
          GradPair taken = res[i].sum();
          GradPair remain = sumGradPair.subtract(taken);
          int defaultBin = featureInfo.getDefaultBin(featLo + i);
          res[i].accumulate(defaultBin, remain);
        }
      }
    } else { // do hist subtraction here to be cache friendly
      for (int i = 0; i < res.length; i++) {
        if (res[i] != null) {
          GradPair taken = res[i].sum();
          GradPair remain = sumGradPair.subtract(taken);
          int defaultBin = featureInfo.getDefaultBin(featLo + i);
          res[i].accumulate(defaultBin, remain);
          subtract[i].subtractBy(res[i]);
        }
      }
    }
    return res;
  }

  public Histogram[] histSubtraction(Histogram[] mined, Histogram[] miner, boolean inPlace) {
    if (inPlace) {
      for (int i = 0; i < mined.length; i++) {
        if (mined[i] != null) {
          mined[i].subtractBy(miner[i]);
        }
      }
      return mined;
    } else {
      Histogram[] res = new Histogram[mined.length];
      for (int i = 0; i < mined.length; i++) {
        if (mined[i] != null) {
          res[i] = mined[i].subtract(miner[i]);
        }
      }
      return res;
    }
  }

  public Option<Histogram>[] buildHistograms(int[] sampleFeats, int featLo,
      Option<FeatureRow>[] featureRows,
      FeatureInfo featureInfo, DataInfo dataInfo, InstanceRow[] instanceRows,
      int nid, GradPair sumGradPair) throws Exception {
    Preconditions.checkArgument(sampleFeats.length == featureRows.length);
    int numFeat = sampleFeats.length;
    Histogram[] histograms = new Histogram[numFeat];
    for (int i = 0; i < numFeat; i++) {
      if (featureRows[i].isDefined()) {
        int nnz = featureRows[i].get().size();
        if (nnz > 0) {
          int fid = featLo + i;
          int numBin = featureInfo.getNumBin(fid);
          histograms[i] = new Histogram(numBin, param.numClass, param.fullHessian, param.isMultiClassMultiTree());
        }
      }
    }
    GradPair[] gradPairs = dataInfo.gradPairs();
    int nodeStart = dataInfo.getNodePosStart(nid);
    int nodeEnd = dataInfo.getNodePosEnd(nid);
    int[] nodeToPos = dataInfo.nodeToIns();
    for (int posId = nodeStart; posId <= nodeEnd; posId++) {
      int insId = nodeToPos[posId];
      if (instanceRows[insId] != null) {
        int[] indices = instanceRows[insId].indices();
        int[] bins = instanceRows[insId].bins();
        int nnz = indices.length;
        for (int j = 0; j < nnz; j++) {
          histograms[indices[j] - featLo].accumulate(bins[j], gradPairs[insId]);
        }
      }
    }
    Option<Histogram>[] res = new Option[histograms.length];
    for (int i = 0; i < histograms.length; i++) {
      if (histograms[i] != null) {
        GradPair taken = histograms[i].sum();
        GradPair remain = sumGradPair.subtract(taken);
        int defaultBin = featureInfo.getDefaultBin(featLo + i);
        histograms[i].accumulate(defaultBin, remain);
        res[i] = Option.apply(histograms[i]);
      } else {
        res[i] = Option.empty();
      }
    }
    return res;
  }

  public Option<Histogram>[] buildHistograms(int[] sampleFeats, int featLo,
      Option<FeatureRow>[] featureRows,
      FeatureInfo featureInfo, DataInfo dataInfo,
      int nid, GradPair sumGradPair) throws Exception {
    Option<Histogram>[] histograms = new Option[sampleFeats.length];
    if (param.numThread > 1) {
      ExecutorService threadPool = Executors.newFixedThreadPool(param.numThread);
      Future[] futures = new Future[param.numThread];
      for (int threadId = 0; threadId < param.numThread; threadId++) {
        futures[threadId] = threadPool.submit(new BuilderThread(threadId, sampleFeats, featLo,
            featureRows, featureInfo, dataInfo, nid, sumGradPair, histograms));
      }
      threadPool.shutdown();
      for (Future future : futures) {
        future.get();
      }
    } else {
      new BuilderThread(0, sampleFeats, featLo, featureRows, featureInfo,
          dataInfo, nid, sumGradPair, histograms).call();
    }
    return histograms;
  }

  private class BuilderThread implements Callable<Void> {

    private final int threadId;
    private final int[] sampleFeats;
    private final int featLo;
    private final Option<FeatureRow>[] featureRows;
    private final FeatureInfo featureInfo;
    private final int nodeStart;
    private final int nodeEnd;
    private final int[] nodeToIns;
    private final int[] insPos;
    private final GradPair[] gradPairs;
    private final GradPair sumGradPair;
    private final Option<Histogram>[] histograms;

    private BuilderThread(int threadId, int[] sampleFeats, int featLo,
        Option<FeatureRow>[] featureRows, FeatureInfo featureInfo,
        DataInfo dataInfo, int nid, GradPair sumGradPair,
        Option<Histogram>[] histograms) {
      this.threadId = threadId;
      this.sampleFeats = sampleFeats;
      this.featLo = featLo;
      this.featureRows = featureRows;
      this.featureInfo = featureInfo;
      this.nodeStart = dataInfo.getNodePosStart(nid);
      this.nodeEnd = dataInfo.getNodePosEnd(nid);
      this.nodeToIns = dataInfo.nodeToIns();
      this.insPos = dataInfo.insPos();
      this.gradPairs = dataInfo.gradPairs();
      this.sumGradPair = sumGradPair;
      this.histograms = histograms;
    }

    @Override
    public Void call() throws Exception {
      int avg = sampleFeats.length / param.numThread;
      int from = threadId * avg;
      int to = threadId + 1 == param.numThread ? featureRows.length : from + avg;

      for (int i = from; i < to; i++) {
        int fid = sampleFeats[i];
        if (featureRows[fid - featLo].isDefined()) {
          FeatureRow featRow = featureRows[fid - featLo].get();
          int[] indices = featRow.indices();
          int[] bins = featRow.bins();
          int nnz = indices.length;
          if (nnz != 0) {
            // 1. allocate histogram
            int numBin = featureInfo.getNumBin(fid);
            Histogram hist = new Histogram(numBin, param.numClass, param.fullHessian, param.isMultiClassMultiTree());
            // 2. loop non-zero instances, accumulate to histogram
            if (true) {
              //if (nnz <= nodeEnd - nodeStart + 1) { // loop all nnz of current feature
              long t2 = System.currentTimeMillis();
              for (int j = 0; j < nnz; j++) {
                int insId = indices[j];
                if (nodeStart <= insPos[insId] && insPos[insId] <= nodeEnd) {
                  int binId = bins[j];
                  hist.accumulate(binId, gradPairs[insId]);
                }
              }
            } else { // for all instance on this node, binary search in feature row
              long t2 = System.currentTimeMillis();
              for (int j = nodeStart; j <= nodeEnd; j++) {
                int insId = nodeToIns[j];
                int index = Arrays.binarySearch(indices, insId);
                if (index >= 0) {
                  int binId = bins[index];
                  hist.accumulate(binId, gradPairs[insId]);
                }
              }
            }
            // 3. add remaining grad and hess to default bin
            GradPair taken = hist.sum();
            GradPair remain = sumGradPair.subtract(taken);
            int defaultBin = featureInfo.getDefaultBin(fid);
            hist.accumulate(defaultBin, remain);
            histograms[i] = Option.apply(hist);
          } else {
            histograms[i] = Option.empty();
          }
        } else {
          histograms[i] = Option.empty();
        }
      }
      return null;
    }
  }
}
