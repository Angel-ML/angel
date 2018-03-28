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
 */

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Dense int matrix that is represented by a group of dense int vector {@link DenseIntVector}
 */
public class DenseIntMatrix extends TIntMatrix {
  private final static Log LOG = LogFactory.getLog(DenseIntMatrix.class);
  /**
   * Crate a new empty matrix.
   *
   * @param row the row number
   * @param col the col number
   */
  public DenseIntMatrix(int row, int col) {
    super(row, col, new DenseIntVector[row]);
  }

  public DenseIntMatrix(int row, int col, DenseIntVector[] vectors) {
    super(row, col, vectors);
  }

  /**
   * Build the matrix
   *
   * @param row row number
   * @param col column number
   * @param values values
   */
  public DenseIntMatrix(int row, int col, int[][] values) {
    this(row, col);
    if (values != null) {
      assert (row == values.length);
      for (int i = 0; i < row; i++) {
        vectors[i] = initVector(i, values[i]);
      }
    }
  }

  /**
   * init the empty vector
   *
   * @param rowIndex row index
   * @return
   */
  @Override
  public DenseIntVector initVector(int rowIndex) {
    DenseIntVector ret = new DenseIntVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

  /**
   * init the vector by set the value
   *
   * @param rowIndex row index
   * @param values the values of row
   * @return
   */
  private DenseIntVector initVector(int rowIndex, int[] values) {
    DenseIntVector ret = new DenseIntVector((int)col, values);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

  public void setRow(int rowIndex, DenseIntVector vect) {
    vect.setMatrixId(matrixId);
    vect.setRowId(rowIndex);
    vectors[rowIndex] = vect;
  }
}
