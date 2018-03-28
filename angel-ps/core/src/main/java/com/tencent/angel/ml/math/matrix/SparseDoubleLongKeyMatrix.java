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

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;

/**
 * Sparse double matrix that is represented by a group of sparse double vector {@link SparseLongKeyDoubleVector}
 */
public class SparseDoubleLongKeyMatrix extends DoubleLongKeyMatrix<SparseLongKeyDoubleVector> {

  /**
   * Create a SparseDoubleLongKeyMatrix
   * @param row row number
   * @param col row vector dimension
   */
  public SparseDoubleLongKeyMatrix(int row, long col) {
    super(row, col, new SparseLongKeyDoubleVector[row]);
  }

  public SparseDoubleLongKeyMatrix(int row, long col, SparseLongKeyDoubleVector[] vectors) {
    super(row, col, vectors);
  }

  @Override public SparseLongKeyDoubleVector initVector(int rowIndex) {
    SparseLongKeyDoubleVector ret = new SparseLongKeyDoubleVector(col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

}
