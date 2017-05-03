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

import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;

/**
 * Double matrix that is represented by a group of dense double vector {@link DenseDoubleVector}
 */
public class DenseDoubleMatrix extends TDoubleMatrix {
  private final DenseDoubleVector[] vectors;

  public DenseDoubleMatrix(int row, int col) {
    super(row, col);
    vectors = new DenseDoubleVector[row];
  }

  /**
   * init the matrix
   * 
   * @param row
   * @param col
   * @param values
   */
  public DenseDoubleMatrix(int row, int col, double[][] values) {
    super(row, col);
    vectors = new DenseDoubleVector[row];
    if (values != null) {
      assert (row == values.length);
      for (int i = 0; i < row; i++) {
        vectors[i] = initVector(i, values[i]);
      }
    }
  }

  /**
   * inc the matrix
   * 
   * @param rowIndex
   * @param colIndex
   * @param value the value
   */
  @Override
  public void inc(int rowIndex, int colIndex, double value) {
    if (vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }
    vectors[rowIndex].inc(colIndex, value);
  }

  /**
   * init the empty vector
   * 
   * @param rowIndex
   * @return
   */
  private DenseDoubleVector initVector(int rowIndex) {
    DenseDoubleVector ret = new DenseDoubleVector(col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
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
    DenseDoubleVector ret = new DenseDoubleVector(col, values);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }

  /**
   * plus the matrix
   * 
   * @param other the other
   */
  @Override
  public void plusBy(TDoubleMatrix other) {
    if (other == null) {
      return;
    }

    assert (row == other.getRowNum());
    assert (col == other.getColNum());
    for (int i = 0; i < row; i++) {
      plusBy(other.getTDoubleVector(i));
    }
  }

  /**
   * plus the vector
   * 
   * @param other the other
   */
  @Override
  public void plusBy(TDoubleVector other) {
    int rowIndex = other.getRowId();
    if (vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }
    vectors[rowIndex].plusBy(other, 1.0);
  }

  /**
   * get the value of one element
   * 
   * @param rowId the row id
   * @param colId the col id
   * @return
   */
  @Override
  public double get(int rowId, int colId) {
    if (vectors[rowId] == null) {
      return 0.0;
    } else {
      return vectors[rowId].get(colId);
    }
  }

  /**
   * get the vector of matrix
   * 
   * @param rowId the row id
   * @return
   */
  @Override
  public TDoubleVector getTDoubleVector(int rowId) {
    return vectors[rowId];
  }

  /**
   * the sparsity of matrix
   * 
   * @return
   */
  @Override
  public double sparsity() {
    return (double) nonZeroNum() / (row * col);
  }

  /**
   * the size of matrix
   * 
   * @return
   */
  @Override
  public int size() {
    return row * col;
  }

  /**
   * clear the matrix
   */
  @Override
  public void clear() {
    for (int i = 0; i < row; i++) {
      if (vectors[i] != null) {
        vectors[i].clear();
      }
    }
  }

  /**
   * the count of nonzero element
   * 
   * @return
   */
  @Override
  public int nonZeroNum() {
    int sum = 0;
    for (int i = 0; i < row; i++) {
      if (vectors[i] != null) {
        sum += vectors[i].nonZeroNumber();
      }
    }
    return sum;
  }
}
