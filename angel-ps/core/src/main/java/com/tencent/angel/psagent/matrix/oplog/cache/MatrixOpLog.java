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
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ml.math.*;
import com.tencent.angel.ml.math.matrix.*;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.math.vector.TFloatVector;
import com.tencent.angel.ml.math.vector.TIntVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.MatrixPartitionRouter;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.storage.MatrixStorage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
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
        deltaVector = getTVector(rowIndex);
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
   * @param psUpdateData ps id -> (partition id -> row split list) map
   */
  public void split(Map<ParameterServerId, Map<PartitionKey, List<RowUpdateSplit>>> psUpdateData) {
    long startTime = System.currentTimeMillis();
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    MatrixPartitionRouter router = PSAgentContext.get().getMatrixPartitionRouter();
    int row = matrixMeta.getRowNum();
    
    // Get partitions for the matrix
    LOG.debug("split update for matrix " + matrixId);
    List<PartitionKey> partitionInfos = router.getPartitionKeyList(matrixId);
    LOG.debug("get matrix partitions from router size = " + partitionInfos.size());
    for(int i = 0; i < partitionInfos.size(); i++) {
      LOG.debug("partitionInfos[" + i + "]=" + partitionInfos.get(i));
    }
    
    for (int rowId = 0; rowId < row; rowId++) {
      TVector vector = getTVector(rowId);
      if (vector == null)
        continue;

      // Filter it, removing zero values
      if(enableFilter){
        vector = vector.filter(0.0);
      }    

      // Doing average or not
      if (matrixMeta.isAverage()) {
        vector.timesBy(1.0 / PSAgentContext.get().getTotalTaskNum());
      }

      // Split this row according the matrix partitions
      Map<PartitionKey, RowUpdateSplit> splits = RowUpdateSplitUtils.split(vector, partitionInfos);

      // Add the splits to the result container
      for (Map.Entry<PartitionKey, RowUpdateSplit> entry : splits.entrySet()) {
        PartitionKey partitionKey = entry.getKey();
        ParameterServerId psId = router.getPSId(partitionKey);
        if (!psUpdateData.containsKey(psId)) {
          psUpdateData.put(psId, new HashMap<PartitionKey, List<RowUpdateSplit>>());
        }
        if (!psUpdateData.get(psId).containsKey(partitionKey)) {
          psUpdateData.get(psId).put(partitionKey, new ArrayList<RowUpdateSplit>());
        }
        psUpdateData.get(psId).get(partitionKey).add(entry.getValue());
      }
    }
      
    LOG.debug( "taking " + (System.currentTimeMillis() - startTime) + " ms to split logs for matrix=" + matrixId);
  }

  /**
   * Merge the update with exist update in cache
   * 
   * @param update a matrix update
   * @throws InvalidParameterException the update type does not match with existed update in cache
   */
  abstract void merge(TUpdate update) throws InvalidParameterException;
  
  /**
   * Get vector from update cache
   * 
   * @param rowIndex row index
   * @return  TVector vector
   */
  abstract TVector getTVector(int rowIndex);
}


/**
 * Dense double matrix cache, it use a dense double matrix {@link DenseDoubleMatrix} as the storage.
 * We can use this type cache if the update is a double matrix or vector
 */
class DenseDoubleMatrixOpLog extends MatrixOpLog {
  private final static Log LOG = LogFactory.getLog(DenseDoubleMatrixOpLog.class);
  
  /** dense double matrix*/
  private final DenseDoubleMatrix matrix;

  public DenseDoubleMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    
    LOG.debug("init DenseDoubleMatrixOpLog for matrix " + matrixId);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new DenseDoubleMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());
  }
  
  public DenseDoubleMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }

  @Override
  public void merge(TUpdate update) throws InvalidParameterException {
    try {
      lock.writeLock().lock();
      if (update instanceof TDoubleVector) {
        matrix.plusBy((TDoubleVector) update);
        return;
      } else if (update instanceof TDoubleMatrix) {
        matrix.plusBy((TDoubleMatrix) update);
        return;
      }

      String errorMsg =
          String.format("can not merge type %s to DenseDoubleMatrix", update.getClass().getName());
      LOG.fatal(errorMsg);
      throw new InvalidParameterException(errorMsg);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public TVector getTVector(int rowIndex) {
    return matrix.getTVector(rowIndex);
  }
}

/**
 * Sparse int matrix cache, it use a sparse int matrix {@link LILIntMatrix} as the storage.
 * We can use this type cache if the update is a int matrix or vector
 */
class LILIntMatrixOpLog extends MatrixOpLog {
  private final static Log LOG = LogFactory.getLog(DenseDoubleMatrixOpLog.class);
  private final LILIntMatrix matrix;

  public LILIntMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("init LILIntMatrixOpLog for matrix " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new LILIntMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());
  }
  
  public LILIntMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }

  @Override
  public void merge(TUpdate update) throws InvalidParameterException {
    try {
      lock.writeLock().lock();
      if (update instanceof TIntVector) {
        matrix.plusBy((TIntVector) update);
        return;
      } else if (update instanceof TIntMatrix) {
        matrix.plusBy((TIntMatrix) update);
        return;
      }

      String errorMsg =
          String.format("can not merge type %s to LILIntMatrix", update.getClass().getName());
      LOG.fatal(errorMsg);
      throw new InvalidParameterException(errorMsg);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public TVector getTVector(int rowIndex) {
    return matrix.getTVector(rowIndex);
  }
}

/**
 * Dense int matrix cache, it use a dense int matrix {@link DenseIntMatrix} as the storage.
 * We can use this type cache if the update is a int matrix or vector
 */
class DenseIntMatrixOpLog extends MatrixOpLog {
  private final static Log LOG = LogFactory.getLog(DenseDoubleMatrixOpLog.class);
  private DenseIntMatrix matrix;

  public DenseIntMatrixOpLog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    LOG.debug("init DenseIntMatrixOpLog for matrix " + matrixId);

    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new DenseIntMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());
  }
  
  public DenseIntMatrixOpLog(int matrixId) {
    this(matrixId, true);
  }

  @Override
  public void merge(TUpdate update) throws InvalidParameterException {
    try {
      lock.writeLock().lock();
      if (update instanceof TIntVector) {
        matrix.plusBy((TIntVector) update);
        return;
      } else if (update instanceof TIntMatrix) {
        matrix.plusBy((TIntMatrix) update);
        return;
      }

      String errorMsg =
          String.format("can not merge type %s to DenseIntMatrixOpLog", update.getClass().getName());
      LOG.fatal(errorMsg);
      throw new InvalidParameterException(errorMsg);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public TVector getTVector(int rowIndex) {
    return matrix.getTVector(rowIndex);
  }
}

/**
 * Dense float matrix cache, it use a dense float matrix {@link DenseIntMatrix} as the storage.
 * We can use this type cache if the update is a float matrix or vector
 */
class DenseFloatMatrixOplog extends MatrixOpLog {
  private final static Log LOG = LogFactory.getLog(DenseFloatMatrixOplog.class);
  private DenseFloatMatrix matrix;

  public DenseFloatMatrixOplog(int matrixId, boolean enableFilter) {
    super(matrixId, enableFilter);
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    matrix = new DenseFloatMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());

    LOG.debug("init DenseFloatMatrixOplog for matrix " + matrixId + " row=" + matrixMeta
        .getRowNum() + " col=" + matrixMeta.getColNum());
  }

  public DenseFloatMatrixOplog(int matrixId) { this(matrixId, true);}

  @Override
  public void merge(TUpdate update) throws InvalidParameterException {
    try {
      lock.writeLock().lock();
      if (update instanceof TFloatVector) {
        matrix.plusBy((TFloatVector) update);
        return;
      } else if (update instanceof TFloatMatrix) {
        matrix.plusBy((TFloatMatrix) update);
        return;
      }

      String errMsg = String.format("can not merge type %s to DenseDoubleMatrix.", update
          .getClass().getName());
      LOG.fatal(errMsg);
      throw new InvalidParameterException(errMsg);
    } finally {
      lock.writeLock().unlock();
    }
  }

  @Override
  public TVector getTVector(int rowIndex) {
    return matrix.getTVector(rowIndex);
  }

  /**
   * Sparse double matrix cache, it use a sparse float matrix {@link DenseIntMatrix} as the storage.
   * We can use this type cache if the update is a double matrix or vector
   */
  class SparseDoubleMatrixOplog extends MatrixOpLog {
    private Log LOG = LogFactory.getLog(MatrixOpLog.class);
    private SparseDoubleMatrix matrix;

    public SparseDoubleMatrixOplog(int matrixId, boolean enableFilter) {
      super(matrixId, enableFilter);
      LOG.debug("Init SparseDoubleMatrixOplog for " + matrixId);

      MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
      matrix = new SparseDoubleMatrix(matrixMeta.getRowNum(), matrixMeta.getColNum());
    }

    @Override
    public void merge(TUpdate update) throws InvalidParameterException {
      try {
        lock.writeLock().lock();
        if (update instanceof TDoubleVector) {
          matrix.plusBy((TDoubleVector) update);
          return;
        } else if (update instanceof TDoubleMatrix) {
          matrix.plusBy((TDoubleMatrix) update);
          return;
        }

        String errMsg = String.format("can not merge type %s to SparseDoubleMatrix.", update
            .getClass().getName());
        LOG.fatal(errMsg);
        throw new InvalidParameterException(errMsg);
      } finally {
        lock.writeLock().unlock();
      }
    }

    @Override
    public TVector getTVector(int rowIndex) {
      return matrix.getTVector(rowIndex);
    }
  }
}
