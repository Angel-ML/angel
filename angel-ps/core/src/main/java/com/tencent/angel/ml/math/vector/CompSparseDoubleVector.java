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
import com.tencent.angel.protobuf.generated.MLProtos;

/**
 * Component sparse double vector. It contains a group of {@link SparseDoubleVector},
 * which correspond to the partitions of the corresponding rows stored on the PS.
 */
public class CompSparseDoubleVector extends CompDoubleVector {
  /**
   *
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param nnz element number of the vector
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex, int dim, int nnz) {
    super(matrixId, rowIndex, dim, nnz);
  }

  /**
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param partKeys the partitions that contains this vector
   * @param vectors vector splits
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex, int dim, PartitionKey[] partKeys, TIntDoubleVector[] vectors) {
    super(matrixId, rowIndex, dim, partKeys, vectors);
  }

  /**
   *
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   *
   * Create a CompSparseDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   */
  public CompSparseDoubleVector(int matrixId, int rowIndex, int dim) {
    this(matrixId, rowIndex, dim, -1);
  }

  @Override public CompSparseDoubleVector clone() {
    TIntDoubleVector[] clonedVectors = new SparseDoubleVector[splitNum];
    for(int i = 0; i < splitNum; i++) {
      if(vectors[i] != null) {
        clonedVectors[i] = (SparseDoubleVector)vectors[i].clone();
      } else {
        clonedVectors[i] = (SparseDoubleVector)initComponentVector();
      }
    }

    CompSparseDoubleVector clonedVector = new CompSparseDoubleVector(matrixId, rowId, dim, partKeys, clonedVectors);
    return clonedVector;
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_COMPONENT;
  }

  @Override protected TIntDoubleVector initComponentVector() {
    return initComponentVector(initCapacity);
  }

  @Override protected TIntDoubleVector initComponentVector(int initCapacity) {
    SparseDoubleVector vector = new SparseDoubleVector(dim, initCapacity);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    vector.setClock(clock);
    return vector;
  }

  @Override protected TIntDoubleVector initComponentVector(TIntDoubleVector vector) {
    if(vector instanceof SparseDoubleVector) {
      return vector.clone();
    }

    throw new UnsupportedOperationException("Unsupport operation: clone from " + vector.getClass().getSimpleName() + " to SparseDoubleVector ");
  }
}
