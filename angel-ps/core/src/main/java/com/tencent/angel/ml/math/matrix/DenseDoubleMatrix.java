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
import com.tencent.angel.ml.math.vector.DenseDoubleVector;

/**
 * Dense double matrix that is represented by a group of dense double vector {@link DenseDoubleVector}
 */
public class DenseDoubleMatrix extends TDoubleMatrix<DenseDoubleVector> {
  public DenseDoubleMatrix(int row, int col) {
    super(row, col, new DenseDoubleVector[row]);
  }

  /**
   * Build the matrix
   *
   * @param row row number
   * @param col column number
   * @param values values
   */
  public DenseDoubleMatrix(int row, int col, double[][] values) {
    this(row, col);
    if (values != null) {
      assert (row == values.length);
      for (int i = 0; i < row; i++) {
        vectors[i] = initVector(i, values[i]);
      }
    }
  }

  @Override
  public DenseDoubleVector initVector(int rowIndex) {
    DenseDoubleVector ret = new DenseDoubleVector((int)col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    vectors[rowIndex] = ret;
    return ret;
  }

  /**
   * init the vector by set the value
   *
   * @param rowIndex
   * @param values
   * @return
   */
  private DenseDoubleVector initVector(int rowIndex, double[] values) {
    DenseDoubleVector ret = new DenseDoubleVector((int)col, values);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

  public void setRow(int rowIndex, double[] values) {
    DenseDoubleVector ret = new DenseDoubleVector((int)col, values);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    vectors[rowIndex] = ret;
  }
}
