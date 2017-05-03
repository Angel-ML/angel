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

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowFunc;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowParam;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.udf.updater.VoidResult;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.storage.MatrixStorage;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.psagent.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SSPConsistencyController implements ConsistencyController {
  private static final Log LOG = LogFactory.getLog(SSPConsistencyController.class);
  private final int staleness;

  public SSPConsistencyController(int staleness) {
    this.staleness = staleness;
  }

  @Override
  public void init() {

  }

  @Override
  public TVector getRow(TaskContext taskContext, int matrixId, int rowIndex) throws Exception {
    // get row from cache
    TVector row = PSAgentContext.get().getMatrixStorageManager().getRow(matrixId, rowIndex);

    // if row clock is satisfy ssp staleness limit, just return.
    if (row != null && (taskContext.getMatrixClock(matrixId) - row.getClock() <= staleness)) {
      LOG.debug("task " + taskContext.getIndex() + " matrix " + matrixId + " clock " + taskContext.getMatrixClock(matrixId)
          + ", row clock " + row.getClock() + ", staleness " + staleness
          + ", just get from global storage");
      return cloneRow(matrixId, rowIndex, row, taskContext);
    }

    // get row from parameterserver
    row =
        PSAgentContext.get().getMatrixClientAdapter()
            .getRow(matrixId, rowIndex, taskContext.getMatrixClock(matrixId) - staleness);

    return cloneRow(matrixId, rowIndex, row, taskContext);
  }

  @Override
  public GetRowsResult getRowsFlow(TaskContext taskContext, RowIndex rowIndex, int rpcBatchSize)
      throws Exception {

    GetRowsResult result = new GetRowsResult();
    int stalnessClock = taskContext.getMatrixClock(rowIndex.getMatrixId()) - staleness;
    findRowsInStorage(result, rowIndex, stalnessClock);
    if (!result.isFetchOver()) {
      LOG.debug("need fetch from parameterserver");
      PSAgentContext.get().getMatrixClientAdapter()
          .getRowsFlow(result, rowIndex, rpcBatchSize, stalnessClock);
    }
    return result;
  }

  @Override
  public Future<VoidResult> clock(TaskContext taskContext, int matrixId, boolean flushFirst) {
    taskContext.increaseMatrixClock(matrixId);
    return PSAgentContext.get().getOpLogCache().clock(taskContext, matrixId, flushFirst);
  }

  public int getStaleness() {
    return staleness;
  }

  private void findRowsInStorage(GetRowsResult result, RowIndex rowIndexes, int stalenessClock)
      throws InterruptedException {
    MatrixStorage storage =
        PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(rowIndexes.getMatrixId());

    for (int rowIndex : rowIndexes.getRowIds()) {
      TVector processRow = storage.getRow(rowIndex);
      if (processRow != null && processRow.getClock() >= stalenessClock) {
        result.put(processRow);
        if (result.getRowsNumber() == rowIndexes.getRowsNumber()) {
          rowIndexes.clearFilted();
          result.fetchOver();
          return;
        }

        rowIndexes.filted(rowIndex);
      }
    }
  }

  @Override
  public GetRowResult getRow(TaskContext taskContext, GetRowFunc func) throws Exception {
    GetRowParam param = func.getParam();

    // get row from cache
    TVector row =
        PSAgentContext.get().getMatrixStorageManager()
            .getRow(param.getMatrixId(), param.getRowIndex());

    // if row clock is satisfy ssp staleness limit, just return.
    if (row != null
        && (taskContext.getMatrixClock(param.getMatrixId()) - row.getClock() <= staleness)) {
      return new GetRowResult(ResponseType.SUCCESS, cloneRow(param.getMatrixId(),
          param.getRowIndex(), row, taskContext));
    }

    // get row from parameterserver
    param.setClock(taskContext.getMatrixClock(param.getMatrixId()) - staleness);
    GetRowResult result = PSAgentContext.get().getMatrixClientAdapter().getRow(func);
    return new GetRowResult(result.getResponseType(), cloneRow(param.getMatrixId(),
        param.getRowIndex(), result.getRow(), taskContext));
  }

  private TVector cloneRow(int matrixId, int rowIndex, TVector row, TaskContext taskContext) {
    if (row == null) {
      return null;
    }

    if (isNeedClone(matrixId)) {
      ReentrantReadWriteLock globalStorage =
          PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(matrixId).getLock();
      TVector taskRow = taskContext.getMatrixStorage().getRow(matrixId, rowIndex);
      try {
        globalStorage.readLock().lock();
        if(taskRow == null || (taskRow.getClass() != row.getClass())){
          taskRow = row.clone();
          taskContext.getMatrixStorage().addRow(matrixId, rowIndex, taskRow);
        }else{
          taskRow.clone(row);
        }
      } finally {
        globalStorage.readLock().unlock();
      }
      return taskRow;
    } else {
      return row;
    }
  }

  private boolean isNeedClone(int matrixId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    int localTaskNum = PSAgentContext.get().getLocalTaskNum();

    if (!matrixMeta.isHogwild() && localTaskNum > 1) {
      return true;
    } else {
      return false;
    }
  }
}
