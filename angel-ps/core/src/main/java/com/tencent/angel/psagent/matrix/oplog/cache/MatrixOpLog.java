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

package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TUpdate;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.matrix.*;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
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
public abstract class MatrixOpLog {
  protected final static Log LOG = LogFactory.getLog(MatrixOpLog.class);

  /** matrix id*/
  protected final int matrixId;

  /**filter zero values before split the update*/
  protected final boolean enableFilter;
  protected final ReentrantReadWriteLock lock;

  protected TMatrix matrix;
  
  /**
   * Create a new MatrixOpLog for matrix.
   *
   * @param matrixId matrix id 
   * @param enableFilter true means filter zero values before split the update
   */
  public MatrixOpLog(int matrixId, boolean enableFilter) {
    this.matrixId = matrixId;
    this.enableFilter = enableFilter;
    this.lock = new ReentrantReadWriteLock();    
  }
  
  /**
   * Create a new MatrixOpLog for matrix.
   *
   * @param matrixId matrix id 
   */
  public MatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
 
  /**
   * Flush the update in cache to local matrix storage
   */
  public void flushToLocalStorage() {
    MatrixStorage storage =
        PSAgentContext.get().getMatrixStorageManager().getMatrixStoage(matrixId);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    int row = matrixMeta.getRowNum();

    TVector deltaVector = null;
    TVector vector = null;

    ReentrantReadWriteLock globalStorageLock = storage.getLock();
    try {
      globalStorageLock.writeLock().lock();
      for (int rowIndex = 0; rowIndex < row; rowIndex++) {
        deltaVector = getRow(rowIndex);
        vector = storage.getRow(rowIndex);
        if (deltaVector == null || vector == null)
          continue;
        vector.plusBy(deltaVector, 1.0 / PSAgentContext.get().getTotalTaskNum());
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
    List<PartitionKey> partitions = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int row = matrixMeta.getRowNum();
    
    for (int rowId = 0; rowId < row; rowId++) {
      TVector vector = getRow(rowId);
      if (vector == null)
        continue;

      // Filter it, removing zero values
      if(enableFilter && isNeedFilter(vector)){
        vector = vector.filter(0.0);
      }    

      // Doing average or not
      if (matrixMeta.isAverage()) {
        vector.timesBy(1.0 / PSAgentContext.get().getTotalTaskNum());
      }

      // Split this row according the matrix partitions
      Map<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, partitions);
      removeRow(rowId);

      // Add the splits to the result container
      for (Map.Entry<PartitionKey, RowUpdateSplit> entry : splits.entrySet()) {
        List<RowUpdateSplit> rowSplits = psUpdateData.get(entry.getKey());
        if(rowSplits == null) {
          rowSplits = new ArrayList<>();
          psUpdateData.put(entry.getKey(), rowSplits);
        }
        rowSplits.add(entry.getValue());
      }
    }
      
    LOG.debug( "taking " + (System.currentTimeMillis() - startTime) + " ms to split logs for matrix=" + matrixId);
  }

  private boolean isNeedFilter(TVector vector) {
    return (vector instanceof DenseIntVector)
      || (vector instanceof DenseDoubleVector)
      || (vector instanceof DenseFloatVector);
  }

  /**
   * Merge the update with exist update in cache
   * 
   * @param update a matrix update
   */
  void merge(TUpdate update) {
    try {
      lock.writeLock().lock();
      if ((update instanceof TVector) && (matrix instanceof RowbaseMatrix)) {
        ((RowbaseMatrix)matrix).plusBy((TVector) update);
        return;
      } else if (update instanceof TMatrix) {
        matrix.plusBy((TMatrix) update);
        return;
      }

      String errorMsg =
        String.format("can not merge type %s to %s ", update.getClass().getName(), matrix.getClass().getName());
      LOG.fatal(errorMsg);
      throw new UnsupportedOperationException(errorMsg);
    } finally {
      lock.writeLock().unlock();
    }
  }
  
  /**
   * Get vector from update cache
   * 
   * @param rowIndex row index
   * @return  TVector vector
   */
  TVector getRow(int rowIndex) {
    if(matrix instanceof RowbaseMatrix) {
      return ((RowbaseMatrix) matrix).getRow(rowIndex);
    } else {
      throw new UnsupportedOperationException("Unsupportted operation");
    }
  }

  /**
   * Remove vector from update cache
   */
  void removeRow(int rowIndex) {
    if(matrix instanceof RowbaseMatrix) {
      ((RowbaseMatrix) matrix).clear(rowIndex);
    } else {
      throw new UnsupportedOperationException("Unsupportted operation");
    }
  }
}


/**
 * Dense double matrix cache, it use a dense double matrix {@link DenseDoubleMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class DenseDoubleMatrixOpLog extends MatrixOpLog {
  public DenseDoubleMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("init DenseDoubleMatrixOpLog for matrix " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new DenseDoubleMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
  }
  
  public DenseDoubleMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Sparse int matrix cache, it use a sparse int matrix {@link SparseIntMatrix} as the storage.
 * We can use this type cache if the update is a int matrix or vector
 */
class SparseIntMatrixOpLog extends MatrixOpLog {
  public SparseIntMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("init SparseIntMatrixOpLog for matrix " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new SparseIntMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
  }
  
  public SparseIntMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Dense int matrix cache, it use a dense int matrix {@link DenseIntMatrix} as the storage.
 * We can use this type cache if the update is a int matrix or vector
 */
class DenseIntMatrixOpLog extends MatrixOpLog {
  public DenseIntMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("init DenseIntMatrixOpLog for matrix " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new DenseIntMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
  }
  
  public DenseIntMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Dense float matrix cache, it use a dense float matrix {@link DenseFloatMatrix} as the storage.
 * We can use this type cache if the update is a float matrix or vector
 */
class DenseFloatMatrixOplog extends MatrixOpLog {
  public DenseFloatMatrixOplog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init DenseFloatMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new DenseFloatMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
  }

  public DenseFloatMatrixOplog(int matrixId) { this(matrixId, true);}
}

/**
 * Sparse float matrix cache, it use a sparse float matrix {@link SparseFloatMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class SparseFloatMatrixOpLog extends MatrixOpLog {
  private final static Log LOG = LogFactory.getLog(SparseFloatMatrixOpLog.class);

  public SparseFloatMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseFloatMatrixOpLog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new SparseFloatMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
  }

  public SparseFloatMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Sparse double matrix cache, it use a sparse float matrix {@link SparseDoubleMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class SparseDoubleMatrixOplog extends MatrixOpLog {
  public SparseDoubleMatrixOplog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new SparseDoubleMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
  }

  public SparseDoubleMatrixOplog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Sparse double with long key matrix cache, it use a sparse float matrix {@link SparseDoubleLongKeyMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class SparseDoubleLongKeyMatrixOpLog extends MatrixOpLog {
  public SparseDoubleLongKeyMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new SparseDoubleLongKeyMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());
    matrix.setMatrixId(matrixId);
  }

  public SparseDoubleLongKeyMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Component parse double with long key matrix cache, it use a component sparse float matrix {@link CompSparseDoubleLongKeyMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class CompSparseDoubleLongKeyMatrixOpLog extends MatrixOpLog {
  public CompSparseDoubleLongKeyMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new CompSparseDoubleLongKeyMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());
    matrix.setMatrixId(matrixId);
  }

  public CompSparseDoubleLongKeyMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Component sparse double with long key matrix cache, it use a component sparse float matrix {@link CompSparseDoubleMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class CompSparseDoubleMatrixOpLog extends MatrixOpLog {
  public CompSparseDoubleMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new CompSparseDoubleMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
    matrix.setMatrixId(matrixId);
  }

  public CompSparseDoubleMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Component sparse float with long key matrix cache, it use a component sparse float matrix {@link CompSparseFloatMatrix} as the storage.
 * We can use this type cache if the update is a float matrix or vector
 */
class CompSparseFloatMatrixOpLog extends MatrixOpLog {
  public CompSparseFloatMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new CompSparseFloatMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
    matrix.setMatrixId(matrixId);
  }

  public CompSparseFloatMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}

/**
 * Component spars double with long key matrix cache, it use a component sparse float matrix {@link CompSparseIntMatrix} as the storage.
 * We can use this type cache if the update is a int matrix or vector
 */
class CompSparseIntMatrixOpLog extends MatrixOpLog {
  public CompSparseIntMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new CompSparseIntMatrix(matrixMeta.getRowNum(), (int)matrixMeta.getColNum());
    matrix.setMatrixId(matrixId);
  }

  public CompSparseIntMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }
}
