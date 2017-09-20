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
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.executor.MatrixOpExecutors;
import com.tencent.angel.protobuf.generated.MLProtos;

/**
 * Component sparse int vector. It contains a group of {@link SparseIntVector},
 * which correspond to the partitions of the corresponding rows stored on the PS.
 */
public class CompSparseIntVector extends CompTIntVector {

  /**
   * Create a CompSparseIntVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompSparseIntVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   * Create a CompSparseIntVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   */
  public CompSparseIntVector(int matrixId, int rowIndex, int dim) {
    this(matrixId, rowIndex, dim, -1);
  }

  /**
   * Create a CompSparseIntVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   * @param nnz      element number of the vector
   */
  public CompSparseIntVector(int matrixId, int rowIndex, int dim, int nnz) {
    super(matrixId, rowIndex, dim, nnz);
  }

  /**
   * Create a CompSparseIntVector
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim      vector dimension
   * @param partKeys the partitions that contains this vector
   * @param vectors  vector splits
   */
  public CompSparseIntVector(int matrixId, int rowIndex, int dim, PartitionKey[] partKeys,
    TIntVector[] vectors) {
    super(matrixId, rowIndex, dim, partKeys, vectors);
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_INT_SPARSE_COMPONENT;
  }

  @Override public TVector clone() {
    TIntVector [] clonedVectors = new TIntVector[splitNum];
    for(int i = 0; i < splitNum; i++) {
      if(vectors[i] != null) {
        clonedVectors[i] = (TIntVector)vectors[i].clone();
      } else {
        clonedVectors[i] = (TIntVector)initComponentVector();
      }
    }
    CompSparseIntVector clonedVector =
      new CompSparseIntVector(matrixId, rowId, dim, partKeys, clonedVectors);
    return clonedVector;
  }

  @Override protected TIntVector initComponentVector() {
    return initComponentVector(initCapacity);
  }

  @Override protected TIntVector initComponentVector(int initCapacity) {
    SparseIntVector vector = new SparseIntVector(dim, initCapacity);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    vector.setClock(clock);
    return vector;
  }

  @Override protected TIntVector initComponentVector(TIntVector vector) {
    if (vector instanceof SparseIntVector) {
      return (SparseIntVector) vector.clone();
    }

    throw new UnsupportedOperationException(
      "Unsupport operation: clone from " + vector.getClass().getSimpleName()
        + " to SparseIntVector ");
  }
}
