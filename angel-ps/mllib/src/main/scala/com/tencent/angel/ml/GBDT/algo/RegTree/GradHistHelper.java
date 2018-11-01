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


package com.tencent.angel.ml.GBDT.algo.RegTree;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.GBDT.algo.GBDTController;
import com.tencent.angel.ml.GBDT.algo.tree.SplitEntry;
import com.tencent.angel.ml.GBDT.param.GBDTParam;
import com.tencent.angel.ml.core.conf.MLConf;
import com.tencent.angel.ml.math2.storage.IntDoubleDenseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.worker.WorkerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GradHistHelper {

  private static final Log LOG = LogFactory.getLog(GradHistHelper.class);

  private GBDTController controller;
  private int nid;

  public GradHistHelper(GBDTController controller, int nid) {
    this.controller = controller;
    this.nid = nid;
  }

  public IntDoubleVector buildHistogram(int insStart, int insEnd) {
    // 1. new feature's histogram (grad + hess)
    // size: sampled_featureNum * (2 * splitNum)
    // in other words, concatenate each feature's histogram
    int featureNum = this.controller.fSet.length;
    int splitNum = this.controller.param.numSplit;
    IntDoubleVector histogram = new IntDoubleVector(featureNum * 2 * splitNum,
            new IntDoubleDenseVectorStorage(new double[featureNum * 2 * splitNum]));

    // 2. get the span of this node
    int nodeStart = insStart;
    int nodeEnd = insEnd; // inclusive
    LOG.debug(String
            .format("Build histogram of node[%d]: size[%d] instance span [%d - %d]", this.nid,
                    histogram.getDim(), nodeStart, nodeEnd));
    // ------ 3. using sparse-aware method to build histogram ---
    // first add grads of all instances to the zero bin of all features, then loop the non-zero entries of all the instances
    float gradSum = 0.0f;
    float hessSum = 0.0f;
    long parseInstanceTime = 0;
    long startTime = System.currentTimeMillis();
    for (int idx = nodeStart; idx <= nodeEnd; idx++) {
      // 3.1. get the instance index
      int insIdx = this.controller.instancePos[idx];
      // 3.2. get the grad and hess of the instance
      GradPair gradPair = this.controller.gradPairs[insIdx];
      // 3.3. add to the sum
      gradSum += gradPair.getGrad();
      hessSum += gradPair.getHess();
      IntFloatVector instance = this.controller.trainDataStore.instances[insIdx];
      int numNnz = instance.getStorage().getIndices().length;
      long tmpTime = System.currentTimeMillis();
      int[] indices = instance.getStorage().getIndices();
      float[] values = instance.getStorage().getValues();
      parseInstanceTime += System.currentTimeMillis() - tmpTime;
      // 3.4. loop the non-zero entries
      for (int i = 0; i < numNnz; i++) {
        int fid = indices[i];
        // 3.4.1. get feature value
        float fv = values[i];
        // 3.4.2. current feature's position in the sampled feature set
        //int fPos = findFidPlace(this.controller.fSet, fid);
        int fPos = this.controller.fPos[fid];
        if (fPos == -1) {
          continue;
        }
        // 3.4.3. find the position of feature value in a histogram
        // the search area in the sketch is [fid * #splitNum, (fid+1) * #splitNum - 1]
        int start = fid * splitNum;
        int end;  // inclusive
        if (this.controller.cateFeatNum.containsKey(fid)) {
          end = start + this.controller.cateFeatNum.get(fid) - 1;
        } else {
          end = start + splitNum - 1;
        }
        int fValueIdx = findFvaluePlace(this.controller.sketches, fv, start, end);
        assert fValueIdx >= 0 && fValueIdx < splitNum;
        int gradIdx = 2 * splitNum * fPos + fValueIdx;
        int hessIdx = gradIdx + splitNum;
        // 3.4.4. add the grad and hess to the corresponding bin
        histogram.set(gradIdx, histogram.get(gradIdx) + gradPair.getGrad());
        histogram.set(hessIdx, histogram.get(hessIdx) + gradPair.getHess());
        // 3.4.5. add the reverse to the bin that contains 0.0f
        int fZeroValueIdx = findFvaluePlace(this.controller.sketches, 0.0f, start, end);
        assert fZeroValueIdx >= 0 && fZeroValueIdx < splitNum;
        int gradZeroIdx = 2 * splitNum * fPos + fZeroValueIdx;
        int hessZeroIdx = gradZeroIdx + splitNum;
        double curGrad = histogram.get(gradZeroIdx);
        double curHess = histogram.get(hessZeroIdx);
        histogram.set(gradZeroIdx, curGrad - gradPair.getGrad());
        histogram.set(hessZeroIdx, curHess - gradPair.getHess());
      }
    }
    // 4. add the grad and hess sum to the zero bin of all features
    for (int fid = 0; fid < featureNum; fid++) {
      int fPos = findFidPlace(this.controller.fSet, fid);
      if (fPos == -1) {
        continue;
      }
      int start = fPos * splitNum;
      int end;
      if (this.controller.cateFeatNum.containsKey(fid)) {
        end = start + this.controller.cateFeatNum.get(fid) - 1;
      } else {
        end = start + splitNum - 1;
      }
      int fZeroValueIdx = findFvaluePlace(this.controller.sketches, 0.0f, start, end);
      int gradZeroIdx = 2 * splitNum * fPos + fZeroValueIdx;
      int hessZeroIdx = 2 * splitNum * fPos + fZeroValueIdx + splitNum;
      histogram.set(gradZeroIdx, histogram.get(gradZeroIdx) + gradSum);
      histogram.set(hessZeroIdx, histogram.get(hessZeroIdx) + hessSum);
    }

    LOG.debug(String.format("Build histogram cost %d ms, parse instance cost %d ms",
            System.currentTimeMillis() - startTime, parseInstanceTime));

    return histogram;
  }

  // find the best split result of the histogram of a tree node
  public SplitEntry findBestSplit(IntDoubleVector histogram) throws Exception {
    LOG.debug(String.format("------To find the best split of node[%d]------", this.nid));
    SplitEntry splitEntry = new SplitEntry();
    LOG.debug(String
            .format("The best split before looping the histogram: fid[%d], fvalue[%f]", splitEntry.fid,
                    splitEntry.fvalue));

    // 1. calculate the gradStats of the root node
    GradStats rootStats = null;
    if (null != histogram) {
      rootStats = calGradStats(histogram);
      // 1.1. update the grad stats of the root node on PS, only called once by leader worker
      if (this.nid == 0) {
        this.controller.updateNodeGradStats(this.nid, rootStats);
      }
    } else {
      LOG.error("null histogram.");
    }

    // 2. loop over features
    if (null == rootStats) {
      LOG.error("null root stat.");
      return splitEntry;
    }

    for (int fid = 0; fid < this.controller.fSet.length; fid++) {
      // 2.1. get the ture feature id in the sampled feature set
      int trueFid = this.controller.fSet[fid];
      // 2.2. get the indexes of histogram of this feature
      int startIdx = 2 * this.controller.param.numSplit * fid;
      // 2.3. find the best split of current feature
      SplitEntry curSplit = findBestSplitOfOneFeature(trueFid, histogram, startIdx, rootStats);
      // 2.4. update the best split result if possible
      splitEntry.update(curSplit);
    }

    // update the grad stats of the root node on PS, only called once by leader worker
    if (this.nid == 0) {
      this.controller.updateNodeGradStats(this.nid, rootStats);
    }

    // 3. update the grad stats of children node
    if (splitEntry.fid != -1) {
      // 3.1. update the left child
      this.controller.updateNodeGradStats(2 * this.nid + 1, splitEntry.leftGradStat);
      // 3.2. update the right child
      this.controller.updateNodeGradStats(2 * this.nid + 2, splitEntry.rightGradStat);
    }

    LOG.debug(String
            .format("The best split after looping the histogram: fid[%d], fvalue[%f], loss gain[%f]",
                    splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
    return splitEntry;
  }

  // find the best split result of one feature
  public SplitEntry findBestSplitOfOneFeature(int fid, IntDoubleVector histogram, int startIdx,
                                              GradStats rootStats) {

    SplitEntry splitEntry = new SplitEntry();
    // 1. set the feature id
    splitEntry.setFid(fid);
    // 2. create the best left stats and right stats
    GradStats bestLeftStat = new GradStats();
    GradStats bestRightStat = new GradStats();

    if (startIdx + 2 * this.controller.param.numSplit <= histogram.getDim()) {
      // 3. the gain of the root node
      float rootGain = rootStats.calcGain(this.controller.param);
      // 4. create the temp left and right grad stats
      GradStats leftStats = new GradStats();
      GradStats rightStats = new GradStats();
      // 5. loop over all the data in histogram
      for (int histIdx = startIdx;
           histIdx < startIdx + this.controller.param.numSplit - 1; histIdx++) {
        // 5.1. get the grad and hess of current hist bin
        float grad = (float) histogram.get(histIdx);
        float hess = (float) histogram.get(this.controller.param.numSplit + histIdx);
        leftStats.add(grad, hess);
        // 5.2. check whether we can split with current left hessian
        if (leftStats.sumHess >= this.controller.param.minChildWeight) {
          // right = root - left
          rightStats.setSubstract(rootStats, leftStats);
          // 5.3. check whether we can split with current right hessian
          if (rightStats.sumHess >= this.controller.param.minChildWeight) {
            // 5.4. calculate the current loss gain
            float lossChg =
                    leftStats.calcGain(this.controller.param) + rightStats.calcGain(this.controller.param)
                            - rootGain;
            // 5.5. check whether we should update the split result with current loss gain
            // split value = sketches[splitIdx]
            int splitIdx = fid * this.controller.param.numSplit + histIdx - startIdx;
            if (splitEntry.update(lossChg, fid, this.controller.sketches[splitIdx])) {
              // 5.6. if should update, also update the best left and right grad stats
              bestLeftStat.update(leftStats.sumGrad, leftStats.sumHess);
              bestRightStat.update(rightStats.sumGrad, rightStats.sumHess);
            }
          }
        }
      }
      // 6. set the best left and right grad stats
      splitEntry.leftGradStat = bestLeftStat;
      splitEntry.rightGradStat = bestRightStat;
    } else {
      LOG.error("index out of grad histogram size.");
    }
    return splitEntry;
  }

  public SplitEntry findBestFromServerSplit(IntDoubleVector histogram) throws Exception {
    LOG.debug(String.format("------To find the best split of node[%d]------", this.nid));
    SplitEntry splitEntry = new SplitEntry();
    LOG.debug(String
            .format("The best split before looping the histogram: fid[%d], fvalue[%f]", splitEntry.fid,
                    splitEntry.fvalue));

    // partition number
    int partitionNum = WorkerContext.get().getConf()
            .getInt(AngelConf.ANGEL_PS_NUMBER, AngelConf.DEFAULT_ANGEL_PS_NUMBER);
    // cols of each partition
    int colPerPartition = histogram.getDim() / partitionNum;
    assert histogram.getDim() == partitionNum * colPerPartition;

    for (int pid = 0; pid < partitionNum; pid++) {
      int startIdx = pid * colPerPartition;
      int splitFid = (int) histogram.get(startIdx);
      if (splitFid == -1) {
        continue;
      }
      int trueSplitFid = this.controller.fSet[splitFid];
      int splitIdx = (int) histogram.get(startIdx + 1);
      float splitValue =
              this.controller.sketches[trueSplitFid * this.controller.param.numSplit + splitIdx];
      float lossChg = (float) histogram.get(startIdx + 2);
      float leftSumGrad = (float) histogram.get(startIdx + 3);
      float leftSumHess = (float) histogram.get(startIdx + 4);
      float rightSumGrad = (float) histogram.get(startIdx + 5);
      float rightSumHess = (float) histogram.get(startIdx + 6);
      LOG.debug(String.format("The best split of the %d-th partition: "
                      + "split feature[%d], split index[%d], split value[%f], loss gain[%f], "
                      + "left sumGrad[%f], left sumHess[%f], right sumGrad[%f], right sumHess[%f]", pid,
              trueSplitFid, splitIdx, splitValue, lossChg, leftSumGrad, leftSumHess, rightSumGrad,
              rightSumHess));
      GradStats curLeftGradStat = new GradStats(leftSumGrad, leftSumHess);
      GradStats curRightGradStat = new GradStats(rightSumGrad, rightSumHess);
      SplitEntry curSplitEntry = new SplitEntry(trueSplitFid, splitValue, lossChg);
      curSplitEntry.leftGradStat = curLeftGradStat;
      curSplitEntry.rightGradStat = curRightGradStat;
      splitEntry.update(curSplitEntry);
    }

    LOG.debug(String
            .format("The best split after looping the histogram: fid[%d], fvalue[%f], loss gain[%f]",
                    splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));

    return splitEntry;

  }

  private void printHistogram(IntDoubleVector histogram, int fid, int splitnum) {
    int start = 2 * fid * splitnum;
    int end = start + splitnum - 1;
    StringBuilder sb = new StringBuilder();
    for (int i = start; i <= end; i++) {
      sb.append(histogram.get(i) + ", ");
    }
    LOG.info(String.format("Histogram of feature %d: %s", fid, sb.toString()));
  }

  private GradStats calGradStats(IntDoubleVector histogram) {
    // 1. calculate the total grad sum and hess sum
    float sumGrad = 0.0f;
    float sumHess = 0.0f;
    for (int i = 0; i < this.controller.param.numSplit; i++) {
      sumGrad += histogram.get(i);
      sumHess += histogram.get(this.controller.param.numSplit + i);
    }
    // 2. create the grad stats of the node
    GradStats rootStats = new GradStats(sumGrad, sumHess);
    return rootStats;
  }

  private static GradStats calGradStats(IntDoubleVector histogram, int startIdx, int splitNum) {
    // 1. calculate the total grad sum and hess sum
    float sumGrad = 0.0f;
    float sumHess = 0.0f;
    for (int i = startIdx; i < startIdx + splitNum; i++) {
      sumGrad += histogram.get(i);
      sumHess += histogram.get(splitNum + i);
    }
    // 2. create the grad stats of the node
    GradStats rootStats = new GradStats(sumGrad, sumHess);
    return rootStats;
  }

  private static GradStats calGradStats(ServerIntDoubleRow row, int startIdx, int splitNum) {
    // 1. calculate the total grad sum and hess sum
    float sumGrad = 0.0f;
    float sumHess = 0.0f;
    for (int i = startIdx; i < startIdx + splitNum; i++) {
      sumGrad += row.get(i);
      sumHess += row.get(splitNum + i);
    }
    // 2. create the grad stats of the node
    GradStats rootStats = new GradStats(sumGrad, sumHess);
    return rootStats;
  }

  private static int findFidPlace(int[] fset, int fid) {
    int low = 0;
    int high = fset.length - 1;
    while (high >= low) {
      int middle = (high + low) / 2;
      if (fset[middle] == fid) {
        return middle;
      } else if (fset[middle] > fid) {
        high = middle - 1;
      } else {
        low = middle + 1;
      }
    }
    return -1;
  }

  private static int findFvaluePlace(float[] sketch, float fvalue, int start, int end) {
    // loop all the possible split value, start from split[0], the first item is the minimal feature value
    //assert fvalue >= sketch[start] && fvalue <= sketch[end];
    int left = start;
    int right = end;
    int mid;
    while (left < right & right <= end) {
      mid = right + (left - right) / 2;
      if (sketch[mid] > fvalue) {
        if (sketch[mid - 1] < fvalue) {
          return mid - 1 - start;
        } else {
          right = mid - 1;
        }
      } else if (sketch[mid] < fvalue) {
        if (sketch[mid + 1] > fvalue) {
          return mid - start;
        } else {
          left = mid + 1;
        }
      } else {
        return mid - start;
      }
    }

    //if (left > end) return end - start;

    return Math.min(left, right) - start;
  }

  // find the best split result of the histogram of a tree node
  public static SplitEntry findBestSplitHelper(IntDoubleVector histogram)
          throws InterruptedException {
    LOG.debug(String
            .format("------To find the best split of histogram size[%d]------", histogram.getDim()));

    SplitEntry splitEntry = new SplitEntry();
    LOG.debug(String
            .format("The best split before looping the histogram: fid[%d], fvalue[%f]", splitEntry.fid,
                    splitEntry.fvalue));

    int featureNum = WorkerContext.get().getConf()
            .getInt(MLConf.ML_FEATURE_INDEX_RANGE(), MLConf.DEFAULT_ML_FEATURE_INDEX_RANGE());
    int splitNum = WorkerContext.get().getConf()
            .getInt(MLConf.ML_GBDT_SPLIT_NUM(), MLConf.DEFAULT_ML_GBDT_SPLIT_NUM());

    if (histogram.getDim() != featureNum * 2 * splitNum) {
      LOG.debug("The size of histogram is not equal to 2 * featureNum*splitNum.");
      return splitEntry;
    }

    for (int fid = 0; fid < featureNum; fid++) {
      // 2.2. get the indexes of histogram of this feature
      int startIdx = 2 * splitNum * fid;
      // 2.3. find the best split of current feature
      SplitEntry curSplit = findBestSplitOfOneFeatureHelper(fid, histogram, startIdx);
      // 2.4. update the best split result if possible
      splitEntry.update(curSplit);
    }

    LOG.debug(String
            .format("The best split after looping the histogram: fid[%d], fvalue[%f], loss gain[%f]",
                    splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
    return splitEntry;
  }

  // find the best split result of one feature
  public static SplitEntry findBestSplitOfOneFeatureHelper(int fid, IntDoubleVector histogram,
                                                           int startIdx) {

    LOG.debug(String.format("Find best split for fid[%d] in histogram size[%d], startIdx[%d]", fid,
            histogram.getDim(), startIdx));

    int splitNum = WorkerContext.get().getConf()
            .getInt(MLConf.ML_GBDT_SPLIT_NUM(), MLConf.DEFAULT_ML_GBDT_SPLIT_NUM());

    SplitEntry splitEntry = new SplitEntry();
    // 1. set the feature id
    // splitEntry.setFid(fid);
    // 2. create the best left stats and right stats
    GradStats bestLeftStat = new GradStats();
    GradStats bestRightStat = new GradStats();

    GradStats rootStats = calGradStats(histogram, startIdx, splitNum);

    GBDTParam param = new GBDTParam();

    if (startIdx + 2 * splitNum <= histogram.getDim()) {
      // 3. the gain of the root node
      float rootGain = rootStats.calcGain(param);
      LOG.debug(String
              .format("Feature[%d]: sumGrad[%f], sumHess[%f], gain[%f]", fid, rootStats.sumGrad,
                      rootStats.sumHess, rootGain));
      // 4. create the temp left and right grad stats
      GradStats leftStats = new GradStats();
      GradStats rightStats = new GradStats();
      // 5. loop over all the data in histogram
      for (int histIdx = startIdx; histIdx < startIdx + splitNum - 1; histIdx++) {
        // 5.1. get the grad and hess of current hist bin
        float grad = (float) histogram.get(histIdx);
        float hess = (float) histogram.get(splitNum + histIdx);
        leftStats.add(grad, hess);
        // 5.2. check whether we can split with current left hessian
        if (leftStats.sumHess >= param.minChildWeight) {
          // right = root - left
          rightStats.setSubstract(rootStats, leftStats);
          // 5.3. check whether we can split with current right hessian
          if (rightStats.sumHess >= param.minChildWeight) {
            // 5.4. calculate the current loss gain
            float lossChg = leftStats.calcGain(param) + rightStats.calcGain(param) - rootGain;
            // 5.5. check whether we should update the split result with current loss gain
            int splitIdx = histIdx - startIdx + 1;
            if (splitEntry.update(lossChg, fid, splitIdx)) {
              // 5.6. if should update, also update the best left and right grad stats
              bestLeftStat.update(leftStats.sumGrad, leftStats.sumHess);
              bestRightStat.update(rightStats.sumGrad, rightStats.sumHess);
            }
          }
        }
      }
      // 6. set the best left and right grad stats
      splitEntry.leftGradStat = bestLeftStat;
      splitEntry.rightGradStat = bestRightStat;
      LOG.debug(String
              .format("Find best split for fid[%d], split feature[%d]: split index[%f], lossChg[%f]", fid,
                      splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
    } else {
      LOG.error("index out of grad histogram size.");
    }
    return splitEntry;
  }

  // find the best split result of a serve row on the PS
  public static SplitEntry findSplitOfServerRow(ServerIntDoubleRow row, GBDTParam param) {
    LOG.debug(String
            .format("------To find the best split from server row[%d], cols[%d-%d]------", row.getRowId(),
                    row.getStartCol(), row.getEndCol()));
    SplitEntry splitEntry = new SplitEntry();
    splitEntry.leftGradStat = new GradStats();
    splitEntry.rightGradStat = new GradStats();
    LOG.debug(String
            .format("The best split before looping the histogram: fid[%d], fvalue[%f]", splitEntry.fid,
                    splitEntry.fvalue));

    int startFid = (int) row.getStartCol() / (2 * param.numSplit);
    int endFid = ((int) row.getEndCol()) / (2 * param.numSplit) - 1;
    LOG.debug(String
            .format("Row split col[%d-%d), start feature[%d], end feature[%d]", row.getStartCol(),
                    row.getEndCol(), startFid, endFid));

    // 2. the fid here is the index in the sampled feature set, rather than the true feature id
    for (int i = 0; startFid + i <= endFid; i++) {
      // 2.2. get the start index in histogram of this feature
      int startIdx = 2 * param.numSplit * i + (int) row.getStartCol();
      // 2.3. find the best split of current feature
      SplitEntry curSplit = findSplitOfFeature(startFid + i, row, startIdx, param);
      // 2.4. update the best split result if possible
      splitEntry.update(curSplit);
    }

    LOG.debug(String
            .format("The best split after looping the histogram: fid[%d], fvalue[%f], loss gain[%f]",
                    splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
    return splitEntry;
  }

  // find the best split result of one feature from a server row, used by the PS
  public static SplitEntry findSplitOfFeature(int fid, ServerIntDoubleRow row, int startIdx,
                                              GBDTParam param) {

    LOG.debug(String
            .format("Find best split for fid[%d] in histogram size[%d], startIdx[%d]", fid, row.size(),
                    startIdx));

    SplitEntry splitEntry = new SplitEntry();
    // 1. set the feature id
    splitEntry.setFid(fid);
    // 2. create the best left stats and right stats
    GradStats bestLeftStat = new GradStats();
    GradStats bestRightStat = new GradStats();

    GradStats rootStats = calGradStats(row, startIdx, param.numSplit);

    if (startIdx + 2 * param.numSplit <= row.getEndCol()) {
      // 3. the gain of the root node
      float rootGain = rootStats.calcGain(param);
      // 4. create the temp left and right grad stats
      GradStats leftStats = new GradStats();
      GradStats rightStats = new GradStats();
      // 5. loop over all the data in histogram
      for (int histIdx = startIdx; histIdx < startIdx + param.numSplit; histIdx++) {
        // 5.1. get the grad and hess of current hist bin
        float grad = (float) row.get(histIdx);
        float hess = (float) row.get(param.numSplit + histIdx);
        leftStats.add(grad, hess);
        // 5.2. check whether we can split with current left hessian
        if (leftStats.sumHess >= param.minChildWeight) {
          // right = root - left
          rightStats.setSubstract(rootStats, leftStats);
          // 5.3. check whether we can split with current right hessian
          if (rightStats.sumHess >= param.minChildWeight) {
            // 5.4. calculate the current loss gain
            float lossChg = leftStats.calcGain(param) + rightStats.calcGain(param) - rootGain;
            // 5.5. check whether we should update the split result with current loss gain
            int splitIdx = histIdx - startIdx;  // split rule: value <= split
            // here we set the fvalue=splitIndex, true split value = sketches[splitIdx+1]
            // the task use index to find fvalue
            if (splitEntry.update(lossChg, fid, splitIdx)) {
              // 5.6. if should update, also update the best left and right grad stats
              bestLeftStat.update(leftStats.sumGrad, leftStats.sumHess);
              bestRightStat.update(rightStats.sumGrad, rightStats.sumHess);
            }
          }
        }
      }
      // 6. set the best left and right grad stats
      splitEntry.leftGradStat = bestLeftStat;
      splitEntry.rightGradStat = bestRightStat;
    } else {
      LOG.error("index out of grad histogram size.");
    }
    return splitEntry;
  }

  public static void main(String[] args) {
    float[] sketch = {0.0f, 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f};
    System.out.println("Result:" + findFvaluePlace(sketch, 0.7f, 0, 6));
  }

}
