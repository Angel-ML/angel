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
 */

package com.tencent.angel.ml.GBDT.psf;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradHistHelper;
import com.tencent.angel.ml.GBDT.algo.RegTree.GradStats;
import com.tencent.angel.ml.GBDT.algo.tree.SplitEntry;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowFunc;
import com.tencent.angel.ml.matrix.psf.get.single.PartitionGetRowResult;
import com.tencent.angel.ml.param.GBDTParam;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.matrix.ResponseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The get row function of <code>Gradient Boosting Decision Tree</code> for gradient histogram
 */
public class GBDTGradHistGetRowFunc extends GetRowFunc {

  public GBDTGradHistGetRowFunc() {
  }

  /**
   * Creates a function.
   *
   * @param param the param
   */
  public GBDTGradHistGetRowFunc(HistAggrParam param) {
    super(param);
  }

  private static final Log LOG = LogFactory.getLog(GBDTGradHistGetRowFunc.class);

  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {

    HistAggrParam.HistPartitionAggrParam param = (HistAggrParam.HistPartitionAggrParam) partParam;

    LOG.info("For the gradient histogram of GBT, we use PS to find the optimal split");

    GBDTParam gbtparam = new GBDTParam();
    gbtparam.numSplit = param.getSplitNum();
    gbtparam.minChildWeight = param.getMinChildWeight();
    gbtparam.regAlpha = param.getRegAlpha();
    gbtparam.regLambda = param.getRegLambda();

    ServerDenseDoubleRow row = (ServerDenseDoubleRow) psContext.getMatrixStorageManager()
      .getRow(param.getMatrixId(), param.getRowId(), param.getPartKey().getPartitionId());

    SplitEntry splitEntry = GradHistHelper.findSplitOfServerRow(row, gbtparam);

    int fid = splitEntry.getFid();
    int splitIndex = (int) splitEntry.getFvalue();
    double lossGain = splitEntry.getLossChg();
    GradStats leftGradStat = splitEntry.leftGradStat;
    GradStats rightGradStat = splitEntry.rightGradStat;
    double leftSumGrad = leftGradStat.sumGrad;
    double leftSumHess = leftGradStat.sumHess;
    double rightSumGrad = rightGradStat.sumGrad;
    double rightSumHess = rightGradStat.sumHess;

    LOG.info(String.format(
      "split of matrix[%d] part[%d] row[%d]: fid[%d], split index[%d], loss gain[%f], "
        + "left sumGrad[%f], left sum hess[%f], right sumGrad[%f], right sum hess[%f]",
      param.getMatrixId(), param.getPartKey().getPartitionId(), param.getRowId(), fid, splitIndex,
      lossGain, leftSumGrad, leftSumHess, rightSumGrad, rightSumHess));

    int startFid = (int) row.getStartCol() / (2 * gbtparam.numSplit);
    //int sendStartCol = startFid * 7; // each split contains 7 doubles
    int sendStartCol = (int) row.getStartCol();
    int sendEndCol = sendStartCol + 7;
    ServerDenseDoubleRow sendRow =
      new ServerDenseDoubleRow(param.getRowId(), sendStartCol, sendEndCol);
    LOG.info(String
      .format("Create server row of split result: row id[%d], start col[%d], end col[%d]",
        param.getRowId(), sendStartCol, sendEndCol));
    sendRow.getData().put(0, fid);
    sendRow.getData().put(1, splitIndex);
    sendRow.getData().put(2, lossGain);
    sendRow.getData().put(3, leftSumGrad);
    sendRow.getData().put(4, leftSumHess);
    sendRow.getData().put(5, rightSumGrad);
    sendRow.getData().put(6, rightSumHess);

    return new PartitionGetRowResult(sendRow);
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    int size = partResults.size();
    List<ServerRow> rowSplits = new ArrayList<ServerRow>(size);
    for (int i = 0; i < size; i++) {
      rowSplits.add(((PartitionGetRowResult) partResults.get(i)).getRowSplit());
    }

    SplitEntry splitEntry = new SplitEntry();

    for (int i = 0; i < size; i++) {
      ServerDenseDoubleRow row =
        (ServerDenseDoubleRow) ((PartitionGetRowResult) partResults.get(i)).getRowSplit();
      int fid = (int) row.getData().get(0);
      if (fid != -1) {
        int splitIndex = (int) row.getData().get(1);
        float lossGain = (float) row.getData().get(2);
        float leftSumGrad = (float) row.getData().get(3);
        float leftSumHess = (float) row.getData().get(4);
        float rightSumGrad = (float) row.getData().get(5);
        float rightSumHess = (float) row.getData().get(6);
        LOG.debug(String.format(
          "psFunc: the best split after looping a split: fid[%d], fvalue[%d], loss gain[%f]"
            + ", leftSumGrad[%f], leftSumHess[%f], rightSumGrad[%f], rightSumHess[%f]", fid,
          splitIndex, lossGain, leftSumGrad, leftSumHess, rightSumGrad, rightSumHess));
        GradStats curLeftGradStat = new GradStats(leftSumGrad, leftSumHess);
        GradStats curRightGradStat = new GradStats(rightSumGrad, rightSumHess);
        SplitEntry curSplitEntry = new SplitEntry(fid, splitIndex, lossGain);
        curSplitEntry.leftGradStat = curLeftGradStat;
        curSplitEntry.rightGradStat = curRightGradStat;
        splitEntry.update(curSplitEntry);
      }
    }

    return new GBDTGradHistGetRowResult(ResponseType.SUCCESS, splitEntry);
  }

}
