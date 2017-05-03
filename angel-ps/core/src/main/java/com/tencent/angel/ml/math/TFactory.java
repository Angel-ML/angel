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

package com.tencent.angel.ml.math;

import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.ml.math.factory.TMatrixFactory;
import com.tencent.angel.ml.math.factory.TVectorFactory;
import com.tencent.angel.ml.math.matrix.COOIntMatrix;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.psagent.PSAgentContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The update-pool factory,manage a object pool for vector,matrix,update.
 */
public class TFactory {
  private static final Log LOG = LogFactory.getLog(TFactory.class);
  private final static ConcurrentHashMap<Integer, GenericObjectPool> sparseDoubleVectorPools;
  private final static ConcurrentHashMap<Integer, GenericObjectPool> denseDoubleVectorPools;
  private final static ConcurrentHashMap<Integer, GenericObjectPool> denseIntVectorPools;
  private final static ConcurrentHashMap<Integer, GenericObjectPool> sparseIntVectorPools;
  private final static ConcurrentMap<String, GenericObjectPool> cooIntMatrixPools;
  private final static boolean poolEnabled;

  static {
    sparseDoubleVectorPools = new ConcurrentHashMap<Integer, GenericObjectPool>();
    denseDoubleVectorPools = new ConcurrentHashMap<Integer, GenericObjectPool>();
    denseIntVectorPools = new ConcurrentHashMap<Integer, GenericObjectPool>();
    sparseIntVectorPools = new ConcurrentHashMap<Integer, GenericObjectPool>();
    cooIntMatrixPools = new ConcurrentHashMap<String, GenericObjectPool>();
    poolEnabled =
        PSAgentContext
            .get()
            .getConf()
            .getBoolean(AngelConfiguration.ANGEL_WORKER_TVECTOR_POOL_ENABLE,
                AngelConfiguration.DEFAULT_ANGEL_WORKER_TVECTOR_POOL_ENABLE);
  }

  /**
   * New sparse double vector.
   *
   * @param dimension the dimension
   * @param initSize  the init size
   * @return the sparse double vector
   */
  public static SparseDoubleVector newSparseDoubleVector(int dimension, int initSize) {
    if (!poolEnabled) {
      return new SparseDoubleVector(dimension, initSize);
    }

    if (!sparseDoubleVectorPools.containsKey(dimension)) {
      sparseDoubleVectorPools.put(dimension, TVectorFactory.initSparseDoubleVectorPool(dimension));
    }

    try {
      return (SparseDoubleVector) sparseDoubleVectorPools.get(dimension).borrowObject();
    } catch (Exception e) {
      LOG.error("borrow sparsedoublevector from pool failed, we will create a new instance ", e);
      return new SparseDoubleVector(dimension, initSize);
    }
  }

  /**
   * New dense double vector.
   *
   * @param dimension the dimension
   * @return the dense double vector
   */
  public static DenseDoubleVector newDenseDoubleVector(int dimension) {
    if (!poolEnabled) {
      return new DenseDoubleVector(dimension);
    }

    if (!denseDoubleVectorPools.containsKey(dimension)) {
      denseDoubleVectorPools.put(dimension, TVectorFactory.initDenseDoubleVectorPool(dimension));
    }

    try {
      return (DenseDoubleVector) denseDoubleVectorPools.get(dimension).borrowObject();
    } catch (Exception e) {
      LOG.error("borrow densedoublevector from pool failed, we will create a new instance ", e);
      return new DenseDoubleVector(dimension);
    }
  }

  /**
   * New sparse int vector.
   *
   * @param dimension the dimension
   * @param initSize  the init size
   * @return the sparse int vector
   */
  public static SparseIntVector newSparseIntVector(int dimension, int initSize) {
    if (!poolEnabled) {
      return new SparseIntVector(dimension, initSize);
    }

    if (!sparseIntVectorPools.containsKey(dimension)) {
      sparseIntVectorPools.put(dimension, TVectorFactory.initSparseIntVectorPool(dimension));
    }

    try {
      return (SparseIntVector) sparseIntVectorPools.get(dimension).borrowObject();
    } catch (Exception e) {
      LOG.error("borrow sparsedoublevector from pool failed, we will create a new instance ", e);
      return new SparseIntVector(dimension, initSize);
    }
  }

  /**
   * New dense int vector.
   *
   * @param dimension the dimension
   * @return the dense int vector
   */
  public static DenseIntVector newDenseIntVector(int dimension) {
    if (!poolEnabled) {
      return new DenseIntVector(dimension);
    }

    if (!denseIntVectorPools.containsKey(dimension)) {
      denseIntVectorPools.put(dimension, TVectorFactory.initDenseIntVectorPool(dimension));
    }

    try {
      return (DenseIntVector) denseIntVectorPools.get(dimension).borrowObject();
    } catch (Exception e) {
      LOG.error("borrow sparsedoublevector from pool failed, we will create a new instance ", e);
      return new DenseIntVector(dimension);
    }
  }

  /**
   * New coo int matrix.
   *
   * @param row the row
   * @param col the col
   * @return the coo int matrix
   */
  public static COOIntMatrix newCOOIntMatrix(int row, int col) {
    if (!poolEnabled) {
      return new COOIntMatrix(row, col);
    }

    String key = String.format("%d_%d", row, col);

    if (!cooIntMatrixPools.containsKey(key)) {
      cooIntMatrixPools.put(key, TMatrixFactory.initCOOIntMatrixPool(row, col));
    }

    try {
      return (COOIntMatrix) cooIntMatrixPools.get(key).borrowObject();
    } catch (Exception e) {
      LOG.error("borrow COOIntMatrix from pool failed, we will create a new instance ", e);
      return new COOIntMatrix(row, col);
    }
  }

  private static void releaseSparseDoubleVector(SparseDoubleVector vector) {
    if (sparseDoubleVectorPools.containsKey(vector.getDimension())) {
      try {
        sparseDoubleVectorPools.get(vector.getDimension()).returnObject(vector);
      } catch (Exception e) {
        LOG.error("return sparsedoublevector from pool failed, just return ", e);
      }
    }
  }

  private static void releaseDenseDoubleVector(DenseDoubleVector vector) {
    if (denseDoubleVectorPools.containsKey(vector.getDimension())) {
      try {
        denseDoubleVectorPools.get(vector.getDimension()).returnObject(vector);
      } catch (Exception e) {
        LOG.error("return sparsedoublevector from pool failed, just return ", e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void releaseDenseIntVector(DenseIntVector vector) {
    if (denseIntVectorPools.containsKey(vector.getDimension())) {
      try {
        denseIntVectorPools.get(vector.getDimension()).returnObject(vector);
      } catch (Exception e) {
        LOG.error("return sparsedoublevector from pool failed, just return ", e);
      }
    }
  }

  /**
   * Release matrix.
   *
   * @param matrix the matrix
   */
  public static void releaseCOOIntMatrix(COOIntMatrix matrix) {
    String key = String.format("%s_%s", matrix.getRowNum(), matrix.getColNum());
    if (cooIntMatrixPools.containsKey(key)) {
      try {
        cooIntMatrixPools.get(key).returnObject(matrix);
      } catch (Exception e) {
        LOG.error("return COOIntMatrix to pool failed, just return ", e);
      }
    }
  }

  /**
   * Release vector.
   *
   * @param vector the vector
   */
  public static void releaseTVector(TVector vector) {
    if (!poolEnabled) {
      return;
    }

    if (vector instanceof DenseDoubleVector) {
      releaseDenseDoubleVector((DenseDoubleVector) vector);
    } else if (vector instanceof SparseDoubleVector) {
      releaseSparseDoubleVector((SparseDoubleVector) vector);
    } else if (vector instanceof DenseIntVector) {
      releaseDenseIntVector((DenseIntVector) vector);
    } else if (vector instanceof SparseIntVector) {
      releaseSparseIntVector((SparseIntVector) vector);
    }
  }

  private static void releaseSparseIntVector(SparseIntVector vector) {
    // TODO Auto-generated method stub

  }

  /**
   * Release matrix.
   *
   * @param matrix the matrix
   */
  public static void releaseTMatrix(TMatrix matrix) {
    if (!poolEnabled)
      return;

    if (matrix instanceof COOIntMatrix) {
      releaseCOOIntMatrix((COOIntMatrix) matrix);
      return;
    }
  }

  /**
   * Release update.
   *
   * @param update the update
   */
  public static void releaseTUpdate(TUpdate update) {
    if (!poolEnabled)
      return;

    if (update instanceof TVector) {
      releaseTVector((TVector) update);
      return;
    }

    if (update instanceof TMatrix) {
      releaseTMatrix((TMatrix) update);
    }

  }
}
