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


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.MatrixConf;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.matrix.RowBasedMatrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.storage.MatrixStorage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Matrix update cache.
 */
public class MatrixOpLog {
  protected final static Log LOG = LogFactory.getLog(MatrixOpLog.class);

  /**
   * matrix id
   */
  protected final int matrixId;
  protected final ReentrantReadWriteLock lock;

  protected Matrix matrix;

  /**
   * Create a new MatrixOpLog for matrix.
   *
   * @param matrixId matrix id
   */
  public MatrixOpLog(int matrixId) {
    this.matrixId = matrixId;
    this.lock = new ReentrantReadWriteLock();
  }

  public void init() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = createMatrix(meta);
  }

  public Matrix getMatrix() {
    return matrix;
  }

  private Matrix createMatrix(MatrixMeta meta) {
    RowType rowType = meta.getRowType();
    String opLogTypeStr = meta.getAttribute(MatrixConf.MATRIX_OPLOG_TYPE);
    RowType opLogType;
    if (opLogTypeStr == null) {
      opLogType = rowType;
    } else {
      opLogType = RowType.valueOf(opLogTypeStr);
    }

    return MatrixFactory
      .createRBMatrix(opLogType, meta.getRowNum(), meta.getColNum(), meta.getBlockColNum());
  }

  /**
   * Flush the update in cache to local matrix storage
   */
  public void flushToLocalStorage() {
    MatrixStorage storage =
      PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(matrixId);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    int row = matrixMeta.getRowNum();

    Vector deltaVector;
    Vector vector;

    ReentrantReadWriteLock globalStorageLock = storage.getLock();
    try {
      globalStorageLock.writeLock().lock();
      for (int rowIndex = 0; rowIndex < row; rowIndex++) {
        deltaVector = getRow(rowIndex);
        vector = storage.getRow(rowIndex);
        if (deltaVector == null || vector == null)
          continue;
        vector.iaxpy(deltaVector, 1.0 / PSAgentContext.get().getTotalTaskNum());
      }
    } finally {
      globalStorageLock.writeLock().unlock();
    }
  }

  /**
   * Split the update according to the matrix partitions
   *
   * @param psUpdateData partition -> row split list map
   */
  public void split(Map<PartitionKey, List<RowUpdateSplit>> psUpdateData) {
    long startTime = System.currentTimeMillis();
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);

    int row = matrixMeta.getRowNum();
    boolean enableFilter = matrixMeta.getAttribute(MatrixConf.MATRIX_OPLOG_ENABLEFILTER,
      MatrixConf.DEFAULT_MATRIX_OPLOG_ENABLEFILTER).equalsIgnoreCase("true");
    double filterThreshold = Double.valueOf(matrixMeta
      .getAttribute(MatrixConf.MATRIX_OPLOG_FILTER_THRESHOLD,
        MatrixConf.DEFAULT_MATRIX_OPLOG_FILTER_THRESHOLD));

    for (int rowId = 0; rowId < row; rowId++) {
      Vector vector = getRow(rowId);
      if (vector == null)
        continue;

      List<PartitionKey> partitions =
          PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowId);

      // Doing average or not
      if (matrixMeta.isAverage()) {
        vector.div(PSAgentContext.get().getTotalTaskNum());
      }

      // Filter un-important update
      if (enableFilter) {
        vector = vector.filter(filterThreshold);
      }

      // Split this row according the matrix partitions
      Map<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, partitions);

      // Set split context
      for (Map.Entry<PartitionKey, RowUpdateSplit> entry : splits.entrySet()) {
        RowUpdateSplitContext context = new RowUpdateSplitContext();
        context.setEnableFilter(enableFilter);
        context.setFilterThreshold(filterThreshold);
        context.setPartKey(entry.getKey());
        entry.getValue().setSplitContext(context);

        List<RowUpdateSplit> rowSplits = psUpdateData.get(entry.getKey());
        if (rowSplits == null) {
          rowSplits = new ArrayList<>();
          psUpdateData.put(entry.getKey(), rowSplits);
        }
        rowSplits.add(entry.getValue());
      }

      // Remove the row from matrix
      removeRow(rowId);
    }

    LOG.debug("taking " + (System.currentTimeMillis() - startTime) + " ms to split logs for matrix="
      + matrixId);
  }

  /**
   * Merge the update with exist update in cache
   *
   * @param update a matrix update
   */
  void merge(Vector update) {
    try {
      lock.writeLock().lock();
      matrix.iadd(update.getRowId(), update);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Merge the update with exist update in cache
   *
   * @param update a matrix update
   */
  void merge(Matrix update) {
    try {
      lock.writeLock().lock();
      matrix.iadd(update);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get vector from update cache
   *
   * @param rowIndex row index
   * @return TVector vector
   */
  Vector getRow(int rowIndex) {
    if (matrix instanceof RowBasedMatrix) {
      return ((RowBasedMatrix) matrix).getRow(rowIndex);
    } else {
      throw new UnsupportedOperationException(
        "get row is unsupported for this type matrix:" + matrix.getClass().getName());
    }
  }

  /**
   * Remove vector from update cache
   */
  void removeRow(int rowIndex) {
    if (matrix instanceof RowBasedMatrix) {
      ((RowBasedMatrix) matrix).clearRow(rowIndex);
    } else {
      throw new UnsupportedOperationException(
        "get row is unsupported for this type matrix:" + matrix.getClass().getName());
    }
  }
}
