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
import com.tencent.angel.protobuf.generated.MLProtos;

/**
 * Component double vector with long key. It contains a group of {@link SparseLongKeyDoubleVector},
 * which correspond to the partitions of the corresponding rows stored on the PS.
 */
public class CompSparseLongKeyDoubleVector extends CompLongKeyDoubleVector {

  /**
   *
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param nnz element number of the vector
   */
  public CompSparseLongKeyDoubleVector(int matrixId, int rowIndex, long dim, long nnz) {
    super(matrixId, rowIndex, dim, nnz);
  }

  /**
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   * @param partKeys the partitions that contains this vector
   * @param vectors vector splits
   */
  public CompSparseLongKeyDoubleVector(int matrixId, int rowIndex, long dim, PartitionKey[] partKeys, TLongDoubleVector[] vectors) {
    super(matrixId, rowIndex, dim, partKeys, vectors);
  }

  /**
   *
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public CompSparseLongKeyDoubleVector(int matrixId, int rowIndex) {
    this(matrixId, rowIndex, -1, -1);
  }

  /**
   *
   * Create a CompSparseLongKeyDoubleVector
   * @param matrixId matrix id
   * @param rowIndex row index
   * @param dim vector dimension
   */
  public CompSparseLongKeyDoubleVector(int matrixId, int rowIndex, long dim) {
    this(matrixId, rowIndex, dim, -1);
  }


  @Override public CompSparseLongKeyDoubleVector clone() {
    SparseLongKeyDoubleVector[] clonedVectors = new SparseLongKeyDoubleVector[splitNum];
    for(int i = 0; i < splitNum; i++) {
      if(vectors[i] != null) {
        clonedVectors[i] = (SparseLongKeyDoubleVector)vectors[i].clone();
      } else {
        clonedVectors[i] = (SparseLongKeyDoubleVector)initComponentVector();
      }
    }

    CompSparseLongKeyDoubleVector clonedVector =
      new CompSparseLongKeyDoubleVector(matrixId, rowId, dim, partKeys,
        clonedVectors);
    return clonedVector;
  }

  @Override protected TLongDoubleVector initComponentVector() {
    return initComponentVector(initCapacity);
  }

  @Override protected TLongDoubleVector initComponentVector(int initCapacity) {
    SparseLongKeyDoubleVector vector = new SparseLongKeyDoubleVector(dim, initCapacity);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    vector.setClock(clock);
    return vector;
  }

  @Override protected TLongDoubleVector initComponentVector(TLongDoubleVector vector) {
    if(vector instanceof SparseLongKeyDoubleVector) {
      return (SparseLongKeyDoubleVector)vector.clone();
    }

    throw new UnsupportedOperationException("Unsupport operation: clone from " + vector.getClass().getSimpleName() + " to SparseLongKeyDoubleVector ");
  }

  @Override public MLProtos.RowType getType() {
    return MLProtos.RowType.T_DOUBLE_SPARSE_LONGKEY;
  }
}
