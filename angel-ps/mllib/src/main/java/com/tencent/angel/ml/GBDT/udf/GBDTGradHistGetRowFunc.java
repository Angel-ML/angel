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

package com.tencent.angel.ml.GBDT.udf;

import com.tencent.angel.ml.GBDT.udf.getrow.GBDTUtils;
import com.tencent.angel.ml.GBDT.udf.getrow.GradStats;
import com.tencent.angel.ml.GBDT.udf.getrow.SplitEntry;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.get.single.*;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.transport.adapter.RowSplitCombineUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * The get row function of <code>Gradient Boosting Decision Tree</code> for gradient histogram
 */
public class GBDTGradHistGetRowFunc extends GetRowFunc {

  public GBDTGradHistGetRowFunc() {}
  /**
   * Creates a function.
   *
   * @param param the param
   */
  public GBDTGradHistGetRowFunc(GetRowParam param) {
    super(param);
  }

  private static final Log LOG = LogFactory.getLog(GBDTGradHistGetRowFunc.class);

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    Configuration conf = PSContext.get().getConf();
    PartitionGetRowParam param = (PartitionGetRowParam) partParam;
    
    LOG.info("For the gradient histogram of GBT, we use PS to find the optimal split");

    int splitNum = conf.getInt(MLConf.ML_GBDT_SPLIT_NUM(),
            MLConf.DEFAULT_ML_GBDT_SPLIT_NUM());

    ServerDenseDoubleRow row =
        (ServerDenseDoubleRow) PSContext.get().getMatrixPartitionManager()
            .getRow(param.getMatrixId(), param.getRowIndex(), param.getPartKey().getPartitionId());

    SplitEntry splitEntry = GBDTUtils.findSplitOfServerRow(row);

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
        param.getMatrixId(), param.getPartKey().getPartitionId(), param.getRowIndex(), fid,
        splitIndex, lossGain, leftSumGrad, leftSumHess, rightSumGrad, rightSumHess));

    int startFid = row.getStartCol() / (2 * splitNum);
    //int sendStartCol = startFid * 7; // each split contains 7 doubles
    int sendStartCol = row.getStartCol();
    int sendEndCol = sendStartCol + 7;
    ServerDenseDoubleRow sendRow =
        new ServerDenseDoubleRow(param.getRowIndex(), sendStartCol, sendEndCol);
    LOG.info(String.format(
        "Create server row of split result: row id[%d], start col[%d], end col[%d]",
        param.getRowIndex(), sendStartCol, sendEndCol));
    sendRow.getData().put(0, fid);
    sendRow.getData().put(1, splitIndex);
    sendRow.getData().put(2, lossGain);
    sendRow.getData().put(3, leftSumGrad);
    sendRow.getData().put(4, leftSumHess);
    sendRow.getData().put(5, rightSumGrad);
    sendRow.getData().put(6, rightSumHess);

    return new PartitionGetRowResult(sendRow);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int size = partResults.size();
    List<ServerRow> rowSplits = new ArrayList<ServerRow>(size);
    for (int i = 0; i < size; i++) {
      rowSplits.add(((PartitionGetRowResult)partResults.get(i)).getRowSplit());
    }

    return new GetRowResult(ResponseType.SUCCESS, RowSplitCombineUtils.combineServerRowSplits(
        rowSplits, getParam().getMatrixId(), ((GetRowParam)getParam()).getRowIndex()));
  }
}
