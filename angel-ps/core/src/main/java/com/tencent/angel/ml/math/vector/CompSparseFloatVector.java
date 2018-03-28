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

package com.tencent.angel.ml.math.vector;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;

/**
 * Component sparse float vector. It contains a group of {@link SparseFloatVector},
 * which correspond to the partitions of the corresponding rows stored on the PS.
 */
public class CompSparseFloatVector extends CompTFloatVector {
  /**
   * Create a CompSparseFloatVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   * @param nnz      element number of the vector
   */
  public CompSparseFloatVector(int matrixId, int rowIndex, int dim, int nnz) {
    super(matrixId, rowIndex, dim, nnz);
  }

  /**
   * Create a CompSparseFloatVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   * @param partKeys the partitions that contains this vector
   * @param vectors  vector splits
   */
  public CompSparseFloatVector(int matrixId, int rowIndex, int dim, PartitionKey[] partKeys,
    TFloatVector[] vectors) {
    super(matrixId, rowIndex, dim, partKeys, vectors);
  }

  /**
   * Create a ComponentSparseDoubleVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompSparseFloatVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   * Create a CompSparseFloatVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   */
  public CompSparseFloatVector(int matrixId, int rowIndex, int dim) {
    this(matrixId, rowIndex, dim, -1);
  }

  @Override public RowType getType() {
    return RowType.T_FLOAT_SPARSE_COMPONENT;
  }

  @Override public CompSparseFloatVector clone() {
    TFloatVector [] clonedVectors = new TFloatVector[splitNum];
    for(int i = 0; i < splitNum; i++) {
      if(vectors[i] != null) {
        clonedVectors[i] = (TFloatVector)vectors[i].clone();
      } else {
        clonedVectors[i] = (TFloatVector)initComponentVector();
      }
    }
    CompSparseFloatVector clonedVector =
      new CompSparseFloatVector(matrixId, rowId, dim, partKeys, clonedVectors);
    return clonedVector;
  }

  @Override
  public TFloatVector elemUpdate(IntFloatElemUpdater updater, ElemUpdateParam param) {
    return null;
  }

  @Override protected TFloatVector initComponentVector() {
    return initComponentVector(initCapacity);
  }

  @Override protected TFloatVector initComponentVector(int initCapacity) {
    SparseFloatVector vector = new SparseFloatVector(dim, initCapacity);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    vector.setClock(clock);
    return vector;
  }

  @Override protected TFloatVector initComponentVector(TFloatVector vector) {
    if (vector instanceof SparseFloatVector) {
      return (TFloatVector) vector.clone();
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: clone from " + vector.getClass().getSimpleName()
        + " to SparseFloatVector ");
  }
}
