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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.psagent.consistency;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowFunc;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.udf.updater.VoidResult;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.MatrixPartitionRouter;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.psagent.matrix.transport.adapter.RowSplitCombineUtils;
import com.tencent.angel.psagent.task.TaskContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

public class AsyncConsistencyController implements ConsistencyController {
  private static final Log LOG = LogFactory.getLog(AsyncConsistencyController.class);

  @Override
  public TVector getRow(TaskContext taskContext, int matrixId, int rowIndex) throws Exception {
    List<ServerRow> rowSplits = new ArrayList<ServerRow>();
    List<Future<ServerRow>> futureRowSplit = new ArrayList<Future<ServerRow>>();

    MatrixPartitionRouter matrixRouter = PSAgentContext.get().getMatrixPartitionRouter();
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    List<PartitionKey> partitions = matrixRouter.getPartitionKeyList(matrixId, rowIndex);
    int size = partitions.size();

    for (int i = 0; i < size; i++) {
      ServerRow rowSplit = getRowSplitFromCache(matrixId, partitions.get(i), rowIndex);
      if (rowSplit == null) {
        futureRowSplit.add(matrixClient.getRowSplit(partitions.get(i), rowIndex, -1));
      } else {
        rowSplits.add(rowSplit);
      }
    }

    size = futureRowSplit.size();
    for (int i = 0; i < size; i++) {
      rowSplits.add(futureRowSplit.get(i).get());
    }

    return RowSplitCombineUtils.combineServerRowSplits(rowSplits, matrixId, rowIndex);
  }

  private ServerRow getRowSplitFromCache(int matrixId, PartitionKey partKey, int rowIndex) {
    return PSAgentContext.get().getMatricesCache().getRowSplit(matrixId, partKey, rowIndex);
  }

  @Override
  public GetRowsResult getRowsFlow(TaskContext taskContext, RowIndex rowIndex, int rpcBatchSize)
      throws Exception {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Future<VoidResult> clock(TaskContext taskContext, int matrixId, boolean flushFirst) {
    taskContext.increaseMatrixClock(matrixId);
    return PSAgentContext.get().getOpLogCache().clock(taskContext, matrixId, flushFirst);
  }

  @Override
  public void init() {
    // TODO Auto-generated method stub

  }

  @Override
  public GetRowResult getRow(TaskContext taskContext, GetRowFunc func) {
    // TODO Auto-generated method stub
    return null;
  }

}
