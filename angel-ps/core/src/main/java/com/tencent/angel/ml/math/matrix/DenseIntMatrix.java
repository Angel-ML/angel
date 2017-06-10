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

import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.ml.math.vector.TIntVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;


/**
 * The Dense int matrix.
 */
public class DenseIntMatrix extends TIntMatrix {

  private final static Log LOG = LogFactory.getLog(DenseIntMatrix.class);

  int[][] values;

  /**
   * Crate a new empty matrix.
   *
   * @param row the row
   * @param col the col
   */
  public DenseIntMatrix(int row, int col) {
    super(row, col);
    values = new int[row][col];
  }

  /**
   * Create a new matrix with <code>values</code>.
   *
   * @param row the row
   * @param col the col
   * @param values the values
   */
  public DenseIntMatrix(int row, int col, int[][] values) {
    super(row, col);
    this.values = values;
  }

  @Override
  public void inc(int rowId, int colId, int value) {
    values[rowId][colId] += value;
  }

  @Override
  public void plusBy(TIntMatrix other) {
    if (other instanceof DenseIntMatrix) {
      plusBy((DenseIntMatrix) other);
      return;
    }

    LOG.error("Unregisterd class type " + other.getClass().getName());
  }

  public void plusBy(DenseIntMatrix other) {
    for (int i = 0; i < row; i++) {
      for (int j = 0; j < col; j++) {
        values[i][j] += other.values[i][j];
      }
    }
  }

  public void plusBy(DenseIntVector other) {
    int rowId = other.getRowId();
    int[] deltas = other.getValues();
    for (int i = 0; i < deltas.length; i++) {
      values[rowId][i] += deltas[i];
    }
  }

  public void plusBy(SparseIntVector other) {
    int rowId = other.getRowId();
    int[] indices = other.getIndices();

    if (this.values[rowId] == null)
      this.values[rowId] = new int[col];
    for (int i = 0; i < indices.length; i++) {
      int value = other.get(indices[i]);
      values[rowId][indices[i]] += value;
    }
  }

  @Override
  public void plusBy(TIntVector other) {
    if (other instanceof DenseIntVector) {
      plusBy((DenseIntVector) other);
      return;
    }
    if (other instanceof SparseIntVector) {
      plusBy((SparseIntVector) other);
      return;
    }

    LOG.error("Unregisterd class type " + other.getClass().getName());
  }

  @Override
  public double sparsity() {
    return ((double) nonZeroNum()) / (row * col);
  }

  @Override
  public int size() {
    return row * col;
  }

  public int[][] getValues() {
    return values;
  }

  @Override
  public int get(int rowId, int colId) {
    return values[rowId][colId];
  }

  @Override
  public TIntVector getTIntVector(int rowId) {
    DenseIntVector vector = new DenseIntVector(col, values[rowId]);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    return vector;
  }

  @Override
  public void clear() {
    for (int i = 0; i < row; i++) {
      Arrays.fill(values[i], 0);
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
      for (int j = 0; j < col; j++) {
        if (values[i][j] != 0)
          sum++;
      }
    }
    return sum;
  }
}
