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
package com.tencent.angel.ml.GBDT.psf;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradStats;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.param.GBDTParam;
import com.tencent.angel.ml.GBDT.algo.tree.SplitEntry;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GBDTUtils {

  private static final Log LOG = LogFactory.getLog(GBDTUtils.class);

  private static GradStats calGradStats(ServerDenseDoubleRow row, int startIdx, int splitNum) {
    // 1. calculate the total grad sum and hess sum
    float sumGrad = 0.0f;
    float sumHess = 0.0f;
    for (int i = startIdx; i < startIdx + splitNum; i++) {
      sumGrad += row.getData().get(i);
      sumHess += row.getData().get(splitNum + i);
    }
    // 2. create the grad stats of the node
    GradStats rootStats = new GradStats(sumGrad, sumHess);
    return rootStats;
  }

  // find the best split result of a serve row on the PS
  public static SplitEntry findSplitOfServerRow(ServerDenseDoubleRow row) {
    LOG.info(String.format("------To find the best split from server row[%d], cols[%d-%d]------",
        row.getRowId(), row.getStartCol(), row.getEndCol()));
    SplitEntry splitEntry = new SplitEntry();
    splitEntry.leftGradStat = new GradStats();
    splitEntry.rightGradStat = new GradStats();

    int splitNum = PSContext.get().getConf().getInt(MLConf.ML_GBDT_SPLIT_NUM(),
                    MLConf.DEFAULT_ML_GBDT_SPLIT_NUM());

    int startFid = (int)row.getStartCol() / (2 * splitNum);
    int endFid = ((int)row.getEndCol() + 1) / (2 * splitNum) - 1;
    LOG.info(String.format("The best split before looping the histogram: "
        + "fid[%d], fvalue[%f], start feature[%d], end feature[%d]",
        splitEntry.fid, splitEntry.fvalue, startFid, endFid));

    // 2. the fid here is the index in the sampled feature set, rather than the true feature id
    for (int i = 0; startFid + i <= endFid; i++) {
      // 2.2. get the start index in histogram of this feature
      int startIdx = 2 * splitNum * i;
      // 2.3. find the best split of current feature
      SplitEntry curSplit = findSplitOfFeature(startFid + i, row, startIdx);
      // 2.4. update the best split result if possible
      splitEntry.update(curSplit);
    }

    LOG.info(String.format(
        "The best split after looping the histogram: fid[%d], fvalue[%f], loss gain[%f]",
        splitEntry.fid, splitEntry.fvalue, splitEntry.lossChg));
    return splitEntry;
  }

  // find the best split result of one feature from a server row, used by the PS
  public static SplitEntry findSplitOfFeature(int fid, ServerDenseDoubleRow row, int startIdx) {

    int splitNum = PSContext.get().getConf().getInt(MLConf.ML_GBDT_SPLIT_NUM(),
                    MLConf.DEFAULT_ML_GBDT_SPLIT_NUM());

    SplitEntry splitEntry = new SplitEntry();
    // 1. set the feature id
    splitEntry.setFid(fid);
    // 2. create the best left stats and right stats
    GradStats bestLeftStat = new GradStats();
    GradStats bestRightStat = new GradStats();

    GradStats rootStats = calGradStats(row, startIdx, splitNum);

    GBDTParam param = new GBDTParam();

    if (startIdx + 2 * splitNum <= row.size()) {
      // 3. the gain of the root node
      float rootGain = rootStats.calcGain(param);
      // 4. create the temp left and right grad stats
      GradStats leftStats = new GradStats();
      GradStats rightStats = new GradStats();
      // 5. loop over all the data in histogram
      for (int histIdx = startIdx; histIdx < startIdx + splitNum - 1; histIdx++) {
        // 5.1. get the grad and hess of current hist bin
        float grad = (float) row.getData().get(histIdx);
        float hess = (float) row.getData().get(splitNum + histIdx);
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
            // tips: here we set the fvalue=splitIndex, true split value = sketches[splitIdx+1]
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
      LOG.error("Index out of grad histogram size.");
    }
    return splitEntry;
  }
}
