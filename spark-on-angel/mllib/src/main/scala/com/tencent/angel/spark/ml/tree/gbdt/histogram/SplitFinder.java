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

import com.tencent.angel.spark.ml.tree.gbdt.metadata.FeatureInfo;
import com.tencent.angel.spark.ml.tree.gbdt.tree.GBTSplit;
import com.tencent.angel.spark.ml.tree.param.GBDTParam;
import com.tencent.angel.spark.ml.tree.split.SplitPoint;
import com.tencent.angel.spark.ml.tree.split.SplitSet;
import com.tencent.angel.spark.ml.tree.util.Maths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

public class SplitFinder {

  private static final Logger LOG = LoggerFactory.getLogger(SplitFinder.class);

  private final GBDTParam param;

  public SplitFinder(GBDTParam param) {
    this.param = param;
  }

  public GBTSplit findBestSplitFP(int featLo,
      Histogram[] histograms,
      FeatureInfo featureInfo,
      GradPair sumGradPair,
      float nodeGain) throws Exception {

    GBTSplit bestSplit = new GBTSplit();
    for (int i = 0; i < histograms.length; i++) {
      if (histograms[i] != null) {
        Histogram histogram = histograms[i];
        int fid = i + featLo;
        boolean isCategorical = featureInfo.isCategorical(fid);
        float[] splits = featureInfo.getSplits(fid);
        int defaultBin = featureInfo.getDefaultBin(fid);
        GBTSplit curSplit = findBestSplitOfOneFeature(fid, isCategorical,
            splits, defaultBin, histogram, sumGradPair, nodeGain);
        bestSplit.update(curSplit);
      }
    }
    return bestSplit;
  }


  public GBTSplit findBestSplit(int[] sampledFeats, Option<Histogram>[] histograms,
      FeatureInfo featureInfo,
      GradPair sumGradPair, float nodeGain) throws Exception {
    GBTSplit bestSplit;
    if (param.numThread > 1) {
      bestSplit = new GBTSplit();
      ExecutorService threadPool = Executors.newFixedThreadPool(param.numThread);
      Future[] futures = new Future[param.numThread];
      for (int threadId = 0; threadId < param.numThread; threadId++) {
        futures[threadId] = threadPool.submit(new FinderThread(threadId,
            sampledFeats, histograms, featureInfo, sumGradPair, nodeGain));
      }
      threadPool.shutdown();
      for (Future<GBTSplit> future : futures) {
        bestSplit.update(future.get());
      }
    } else {
      bestSplit = new FinderThread(0, sampledFeats, histograms,
          featureInfo, sumGradPair, nodeGain).call();
    }
    return bestSplit;
  }

  public GBTSplit findBestSplitOfOneFeature(int fid, boolean isCategorical, float[] splits,
      int defaultBin,
      Histogram histogram, GradPair sumGradPair, float nodeGain) {
    if (isCategorical) {
      return findBestSplitSet(fid, splits, defaultBin, histogram, sumGradPair, nodeGain);
    } else {
      return findBestSplitPoint(fid, splits, defaultBin, histogram, sumGradPair, nodeGain);
    }
  }

  // TODO: use more schema on default bin
  private GBTSplit findBestSplitPoint(int fid, float[] splits, int defaultBin, Histogram histogram,
      GradPair sumGradPair, float nodeGain) {
    SplitPoint splitPoint = new SplitPoint();
    GradPair leftStat = param.numClass == 2 ? new BinaryGradPair()
        : new MultiGradPair(param.numClass, param.fullHessian);
    GradPair rightStat = sumGradPair.copy();
    GradPair bestLeftStat = null, bestRightStat = null;
    for (int i = 0; i < histogram.getNumBin() - 1; i++) {
      histogram.plusTo(leftStat, i);
      histogram.subtractTo(rightStat, i);
      if (leftStat.satisfyWeight(param) && rightStat.satisfyWeight(param)) {
        float lossChg = leftStat.calcGain(param) + rightStat.calcGain(param)
            - nodeGain - param.regLambda;
        if (splitPoint.needReplace(lossChg)) {
          splitPoint.setFid(fid);
          splitPoint.setFvalue(splits[i + 1]);
          splitPoint.setGain(lossChg);
          bestLeftStat = leftStat.copy();
          bestRightStat = rightStat.copy();
        }
      }
    }
    return new GBTSplit(splitPoint, bestLeftStat, bestRightStat);
  }

  private GBTSplit findBestSplitSet(int fid, float[] splits, int defaultBin, Histogram histogram,
      GradPair sumGradPair, float nodeGain) {
    // 1. set default bin to left child
    GradPair leftStat = histogram.get(defaultBin).copy();
    GradPair rightStat = null;
    // 2. for other bins, find its location
    int firstFlow = -1, curFlow = -1, curSplitId = 0;
    List<Float> edges = new ArrayList<>();
    edges.add(Float.NEGATIVE_INFINITY);
    for (int i = 0; i < histogram.getNumBin(); i++) {
      if (i == defaultBin) {
        continue; // skip default bin
      }
      GradPair binGradPair = histogram.get(i);
      int flowTo = binFlowTo(sumGradPair, leftStat, binGradPair);
      if (flowTo == 0) {
        leftStat.plusBy(binGradPair);
      }
      if (firstFlow == -1) {
        firstFlow = flowTo;
        curFlow = flowTo;
      } else if (flowTo != curFlow) {
        edges.add(splits[curSplitId]);
        curFlow = flowTo;
      }
      curSplitId++;
    }
    // 3. create split set
    if (edges.size() > 1 || curFlow != 0) { // whether all bins go to the same direction
      rightStat = sumGradPair.subtract(leftStat);
      if (leftStat.satisfyWeight(param) && rightStat.satisfyWeight(param)) {
        float splitGain = leftStat.calcGain(param) + rightStat.calcGain(param)
            - nodeGain - param.regLambda;
        if (splitGain > 0.0f) {
          SplitSet splitSet = new SplitSet(fid, splitGain, Maths.floatListToArray(edges),
              firstFlow, 0);
          return new GBTSplit(splitSet, leftStat, rightStat);
        }
      }
    }
    return new GBTSplit();
  }

  private int binFlowTo(GradPair sumGradPair, GradPair leftStat, GradPair binGradPair) {
    if (param.numClass == 2) {
      double sumGrad = ((BinaryGradPair) sumGradPair).getGrad();
      double leftGrad = ((BinaryGradPair) leftStat).getGrad();
      double binGrad = ((BinaryGradPair) binGradPair).getGrad();
      return binGrad * (2 * leftGrad + binGrad - sumGrad) >= 0.0 ? 0 : 1;
    } else {
      double[] sumGrad = ((MultiGradPair) sumGradPair).getGrad();
      double[] leftGrad = ((MultiGradPair) leftStat).getGrad();
      double[] binGrad = ((MultiGradPair) binGradPair).getGrad();
      double[] tmp = new double[param.numClass];
      for (int i = 0; i < param.numClass; i++) {
        tmp[i] = 2 * leftGrad[i] + binGrad[i] - sumGrad[i];
      }
      return Maths.dot(binGrad, tmp) >= 0.0 ? 0 : 1;
    }
  }

  private class FinderThread implements Callable<GBTSplit> {

    private final int threadId;
    private final int[] sampledFeats;
    private final Option<Histogram>[] histograms;
    private final FeatureInfo featureInfo;
    private final GradPair sumGradPair;
    private final float nodeGain;

    FinderThread(int threadId, int[] sampledFeats, Option<Histogram>[] histograms,
        FeatureInfo featureInfo, GradPair sumGradPair, float nodeGain) {
      this.threadId = threadId;
      this.sampledFeats = sampledFeats;
      this.histograms = histograms;
      this.featureInfo = featureInfo;
      this.sumGradPair = sumGradPair;
      this.nodeGain = nodeGain;
    }

    @Override
    public GBTSplit call() throws Exception {
      int avg = sampledFeats.length / param.numThread;
      int from = threadId * avg;
      int to = threadId + 1 == param.numThread ? histograms.length : from + avg;

      GBTSplit myBestSplit = new GBTSplit();
      for (int i = from; i < to; i++) {
        if (histograms[i].isDefined()) {
          Histogram histogram = histograms[i].get();
          int fid = sampledFeats[i];
          boolean isCategorical = featureInfo.isCategorical(fid);
          float[] splits = featureInfo.getSplits(fid);
          int defaultBin = featureInfo.getDefaultBin(fid);
          GBTSplit curSplit = findBestSplitOfOneFeature(fid, isCategorical,
              splits, defaultBin, histogram, sumGradPair, nodeGain);
          myBestSplit.update(curSplit);
        }
      }

      return myBestSplit;
    }
  }

}
