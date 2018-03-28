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

import com.google.protobuf.ServiceException;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.TIntDoubleVector;
import com.tencent.angel.ml.math.vector.TLongDoubleVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.*;
import com.tencent.angel.ml.matrix.psf.get.multi.GetRowsFunc;
import com.tencent.angel.ml.matrix.psf.get.multi.GetRowsParam;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowFunc;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowParam;
import com.tencent.angel.ml.matrix.psf.get.single.GetRowResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.psagent.matrix.storage.MatrixStorage;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.psagent.task.TaskContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Angel task consistency controller, Angel support 3 consistency protocol: BSP, SSP, ASYNC. If
 * stalenss > 0, means SSP if staleness = 0, means BSP if staleness < 0, means ASYNC
 */
public class ConsistencyController {
  private static final Log LOG = LogFactory.getLog(ConsistencyController.class);
  
  /**staleness value*/
  private final int globalStaleness;

  /**
   * Create a new ConsistencyController.
   *
   * @param staleness staleness value
   */
  public ConsistencyController(int staleness) {
    this.globalStaleness = staleness;
  }

  /**
   * Init.
   */
  public void init() {

  }

  /**
   * Get row from storage/cache or pss.
   * 
   * @param taskContext task context
   * @param matrixId matrix id
   * @param rowIndex row index
   * @return TVector matrix row
   * @throws Exception
   */
  public TVector getRow(TaskContext taskContext, int matrixId, int rowIndex) throws Exception {
    int staleness = getStaleness(matrixId);
    if(staleness >= 0) {
      // Use simple flow, do not use any cache
      if(staleness == 0 && PSAgentContext.get().getLocalTaskNum() == 1) {
        waitForClock(matrixId, rowIndex, taskContext.getMatrixClock(matrixId));
        return ((GetRowResult)PSAgentContext.get().getMatrixClientAdapter().get(new GetRowFunc(new GetRowParam(matrixId, rowIndex)))).getRow();
      }

      // Get row from cache.
      TVector row = PSAgentContext.get().getMatrixStorageManager().getRow(matrixId, rowIndex);

      // if row clock is satisfy ssp staleness limit, just return.
      if (row != null
        && (taskContext.getPSMatrixClock(matrixId) <= row.getClock())
        && (taskContext.getMatrixClock(matrixId) - row.getClock() <= staleness)) {
        LOG.debug("task " + taskContext.getIndex() + " matrix " + matrixId + " clock " + taskContext.getMatrixClock(matrixId)
            + ", row clock " + row.getClock() + ", staleness " + staleness
            + ", just get from global storage");
        return cloneRow(matrixId, rowIndex, row, taskContext);
      }

      // Get row from ps.
      row =
          PSAgentContext.get().getMatrixClientAdapter()
              .getRow(matrixId, rowIndex, taskContext.getMatrixClock(matrixId) - staleness);

      return cloneRow(matrixId, rowIndex, row, taskContext);
    } else {
      // For ASYNC mode, just get from pss.
      GetRowFunc func = new GetRowFunc(new GetRowParam(matrixId, rowIndex));
      GetRowResult result = ((GetRowResult) PSAgentContext.get().getMatrixClientAdapter().get(func));
      if(result.getResponseType() == ResponseType.FAILED) {
        throw new IOException("get row from ps failed.");
      } else {
        return result.getRow();
      }
    }
  }

  /**
   * Get a batch of row from storage/cache or pss.
   * 
   * @param taskContext task context
   * @param rowIndex row indexes
   * @param rpcBatchSize fetch row number in one rpc request
   * @return GetRowsResult rows
   * @throws Exception
   */
  public GetRowsResult getRowsFlow(TaskContext taskContext, RowIndex rowIndex, int rpcBatchSize)
      throws Exception {
    GetRowsResult result = new GetRowsResult();
    if(rowIndex.getRowsNumber() == 0) {
      LOG.error("need get rowId set is empty, just return");
      result.fetchOver();
      return result;
    }

    int staleness = getStaleness(rowIndex.getMatrixId());
    if (staleness >= 0) {
      // For BSP/SSP, get rows from storage/cache first
      int stalnessClock = taskContext.getMatrixClock(rowIndex.getMatrixId()) - staleness;
      findRowsInStorage(taskContext, result, rowIndex, stalnessClock);
      if (!result.isFetchOver()) {
        LOG.debug("need fetch from parameterserver");
        // Get from ps.
        PSAgentContext.get().getMatrixClientAdapter()
            .getRowsFlow(result, rowIndex, rpcBatchSize, stalnessClock);
      }
      return result;
    } else {
      //For ASYNC, just get rows from pss.
      IntOpenHashSet rowIdSet = rowIndex.getRowIds();
      List<Integer> rowIndexes = new ArrayList<Integer>(rowIdSet.size());
      rowIndexes.addAll(rowIdSet);
      GetRowsFunc func = new GetRowsFunc(new GetRowsParam(rowIndex.getMatrixId(), rowIndexes));
      com.tencent.angel.ml.matrix.psf.get.multi.GetRowsResult funcResult =
          ((com.tencent.angel.ml.matrix.psf.get.multi.GetRowsResult) PSAgentContext.get()
              .getMatrixClientAdapter().get(func));
      
      if(funcResult.getResponseType() == ResponseType.FAILED) {
        throw new IOException("get rows from ps failed.");
      } else {
        Map<Integer, TVector> rows = funcResult.getRows();
        for(Entry<Integer, TVector> rowEntry : rows.entrySet()) {
          result.put(rowEntry.getValue());
        }
        result.fetchOver();
        
        return result;
      }
    }
  }

  /**
   * Update clock for a matrix.
   * 
   * @param taskContext task context
   * @param matrixId matrix id
   * @param flushFirst flush matrix oplog first or not
   * @return Future<VoidResult> clock result future
   */
  public Future<VoidResult> clock(TaskContext taskContext, int matrixId, boolean flushFirst) {
    taskContext.increaseMatrixClock(matrixId);
    return PSAgentContext.get().getOpLogCache().clock(taskContext, matrixId, flushFirst);
  }

  /**
   * Get staleness value.
   * 
   * @return int staleness value
   */
  public int getStaleness() {
    return globalStaleness;
  }
  
  /**
   * Get staleness value for the matrix.
   * 
   * @param matrixId matrix id
   * @return int staleness value
   */
  public int getStaleness(int matrixId) {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if(meta == null || meta.getAttribute(MatrixConf.MATRIX_STALENESS) == null) {
      return globalStaleness;
    } else {
      try{
        return Integer.valueOf(meta.getAttribute(MatrixConf.MATRIX_STALENESS));
      } catch (Exception x) {
        LOG.warn("parse matrix staleness value failed for matrix " + matrixId, x);
        return globalStaleness;
      }
    }
  }

  private void findRowsInStorage(TaskContext taskContext, GetRowsResult result, RowIndex rowIndexes, int stalenessClock)
      throws InterruptedException {
    MatrixStorage storage =
        PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(rowIndexes.getMatrixId());

    for (int rowIndex : rowIndexes.getRowIds()) {
      TVector processRow = storage.getRow(rowIndex);
      if (processRow != null
        && (taskContext.getPSMatrixClock(rowIndexes.getMatrixId()) <= processRow.getClock())
        && (processRow.getClock() >= stalenessClock)) {
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
        } else {
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

    return !matrixMeta.isHogwild() && localTaskNum > 1;
  }

  /**
   * Get row use index
   * @param taskContext task context
   * @param func index get psf
   * @return the need row
   * @throws Exception
   */
  public TIntDoubleVector getRow(TaskContext taskContext, IndexGetFunc func) throws Exception{
    int matrixId = func.getParam().getMatrixId();
    int rowIndex = ((IndexGetParam)func.getParam()).getRowId();
    int staleness = getStaleness(matrixId);
    if(staleness >= 0) {
      waitForClock(matrixId, rowIndex, taskContext.getMatrixClock(matrixId) - staleness);
    }
    return (TIntDoubleVector)((GetRowResult)PSAgentContext.get().getMatrixClientAdapter().get(func)).getRow();
  }

  /**
   * Get row use index
   * @param taskContext task context
   * @param func index get psf
   * @return the need row
   * @throws Exception
   */
  public TLongDoubleVector getRow(TaskContext taskContext, LongIndexGetFunc func) throws Exception{
    int matrixId = func.getParam().getMatrixId();
    int rowIndex = ((LongIndexGetParam)func.getParam()).getRowId();
    int staleness = getStaleness(matrixId);
    if(staleness >= 0) {
      waitForClock(matrixId, rowIndex, taskContext.getMatrixClock(matrixId) - staleness);
    }
    return (TLongDoubleVector)((GetRowResult)PSAgentContext.get().getMatrixClientAdapter().get(func)).getRow();
  }

  /**
   * Wait for clock for the row of the matrix
   * TODO:check success task instead
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param clock clock value
   */
  public void waitForClock(int matrixId, int rowIndex, int clock) {
    LOG.info("wait for clock " + clock);
    ClockCache clockCache = PSAgentContext.get().getClockCache();
    int clockUpdateIntervalMs = PSAgentContext.get().getConf().getInt(
      AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS,
      AngelConf.DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS);
    int checkMasterIntervalMs = clockUpdateIntervalMs * 2;
    long startTs = System.currentTimeMillis();
    while (true) {
      int cachedClock = clockCache.getClock(matrixId, rowIndex);
      if (cachedClock >= clock) {
        return;
      }

      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        LOG.warn("waitForClock is interrupted " + e.getMessage());
        return;
      }

      if(System.currentTimeMillis() - startTs > checkMasterIntervalMs) {
        try {
          if(PSAgentContext.get().getMasterClient().getSuccessWorkerGroupNum() >= 1) {
            LOG.info("Some Worker run success, do not need wait");
            return;
          }
        } catch (ServiceException e) {
          LOG.error("getSuccessWorkerGroupNum from Master falied ", e);
        }
        startTs = System.currentTimeMillis();
      }
    }
  }
}
