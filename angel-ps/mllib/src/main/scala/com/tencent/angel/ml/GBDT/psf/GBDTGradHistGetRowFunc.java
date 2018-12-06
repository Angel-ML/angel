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


package com.tencent.angel.ml.GBDT.psf;

import com.tencent.angel.ml.GBDT.algo.RegTree.GradHistHelper;
import com.tencent.angel.ml.GBDT.algo.RegTree.GradStats;
import com.tencent.angel.ml.GBDT.algo.tree.SplitEntry;
import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.get.getrow.PartitionGetRowResult;
import com.tencent.angel.ml.GBDT.param.GBDTParam;
import com.tencent.angel.ps.storage.vector.ServerIntDoubleRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.matrix.ResponseType;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The get row function of <code>Gradient Boosting Decision Tree</code> for gradient histogram
 */
public class GBDTGradHistGetRowFunc extends GetFunc {

  public GBDTGradHistGetRowFunc() {
    super(null);
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

    LOG.info("For the gradient histogram of GBDT, we use PS to find the optimal split");

    GBDTParam gbtparam = new GBDTParam();
    gbtparam.numSplit = param.getSplitNum();
    gbtparam.minChildWeight = param.getMinChildWeight();
    gbtparam.regAlpha = param.getRegAlpha();
    gbtparam.regLambda = param.getRegLambda();

    ServerIntDoubleRow row = (ServerIntDoubleRow) psContext.getMatrixStorageManager()
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
    int sendStartCol = startFid * 7; // each split contains 7 doubles
    //int sendStartCol = (int) row.getStartCol();
    int sendEndCol = sendStartCol + 7;
    ServerIntDoubleRow sendRow =
      new ServerIntDoubleRow(param.getRowId(), RowType.T_DOUBLE_DENSE, sendStartCol, sendEndCol,
        sendEndCol - sendStartCol);
    LOG.info(String
      .format("Create server row of split result: row id[%d], start col[%d], end col[%d]",
        param.getRowId(), sendStartCol, sendEndCol));
    sendRow.set(0 + sendStartCol, fid);
    sendRow.set(1 + sendStartCol, splitIndex);
    sendRow.set(2 + sendStartCol, lossGain);
    sendRow.set(3 + sendStartCol, leftSumGrad);
    sendRow.set(4 + sendStartCol, leftSumHess);
    sendRow.set(5 + sendStartCol, rightSumGrad);
    sendRow.set(6 + sendStartCol, rightSumHess);

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
      ServerIntDoubleRow row =
        (ServerIntDoubleRow) ((PartitionGetRowResult) partResults.get(i)).getRowSplit();
      int fid = (int) row.get(0 + (int) row.getStartCol());
      if (fid != -1) {
        int splitIndex = (int) row.get(1 + (int) row.getStartCol());
        float lossGain = (float) row.get(2 + (int) row.getStartCol());
        float leftSumGrad = (float) row.get(3 + (int) row.getStartCol());
        float leftSumHess = (float) row.get(4 + (int) row.getStartCol());
        float rightSumGrad = (float) row.get(5 + (int) row.getStartCol());
        float rightSumHess = (float) row.get(6 + (int) row.getStartCol());
        LOG.info(String.format(
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
