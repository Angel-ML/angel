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
import com.tencent.angel.ml.math.vector.SparseDoubleVector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;

import java.util.Arrays;

/**
 * The Sparse double matrix.
 */
public class SparseDoubleMatrix extends TDoubleMatrix {

  private static final Log LOG = LogFactory.getLog(SparseDoubleMatrix.class);

  Int2DoubleOpenHashMap[] values;

  public SparseDoubleMatrix(int row, int col) {
    super(row, col);
    values = new Int2DoubleOpenHashMap[row];
    for (int i = 0; i < row; i++) {
      values[i] = new Int2DoubleOpenHashMap();
    }
  }

  @Override
  public void inc(int rowId, int colId, double value) {
    if (values[rowId] == null) {
      values[rowId] = new Int2DoubleOpenHashMap();
    }

    double old = values[rowId].get(colId);
    values[rowId].put(colId, old + value);
  }

  /**
   * Increases specified elements by values.
   *
   * @param rowIds the row ids
   * @param colIds the col ids
   * @param values the values
   */
  public void inc(int[] rowIds, int[] colIds, double[] values) {
    inc(rowIds, colIds, values, 0, rowIds.length);
  }

  /**
   * Increases specified elements by values restrictively.
   *
   * @param rowIds the row ids
   * @param colIds the col ids
   * @param values the values
   * @param start the start of row
   * @param end the end of column
   */
  public void inc(int[] rowIds, int[] colIds, double[] values, int start, int end) {
    for (int i = start; i < end; i++) {
      int rowId = rowIds[i];
      int colId = colIds[i];
      double value = values[i];

      if (this.values[rowId] == null) {
        this.values[rowId] = new Int2DoubleOpenHashMap();
      }

      double old = this.values[rowId].get(colId);
      this.values[rowId].put(colId, old + value);
    }
  }

  /**
   * Increases specified row by values.
   *
   * @param rowId the row id
   * @param colIds the col ids
   * @param values the values
   */
  public void inc(int rowId, int[] colIds, double[] values) {
    inc(rowId, colIds, values, 0, colIds.length);
  }

  /**
   * Increases specified row by values restrictively.
   *
   * @param rowId the row id
   * @param colIds the col ids
   * @param values the values
   * @param start the start
   * @param end the end
   */
  public void inc(int rowId, int[] colIds, double[] values, int start, int end) {
    if (this.values[rowId] == null) {
      this.values[rowId] = new Int2DoubleOpenHashMap();
    }

    for (int i = start; i < end; i++) {
      int colId = colIds[i];
      double value = values[i];
      double old = this.values[rowId].get(colId);

      this.values[rowId].put(colId, old + value);
    }
  }

  /**
   * plus the matrix
   * 
   * @param other the other
   */
  @Override
  public void plusBy(TDoubleMatrix other) {

    LOG.error(String.format("Unsupport Method", other.getClass().getName()));
  }


  @Override
  public void plusBy(TDoubleVector other) {
    if (other instanceof DenseDoubleVector) {
      plusBy((DenseDoubleVector) other);
      return;
    }

    if (other instanceof SparseDoubleVector) {
      plusBy((SparseDoubleVector) other);
      return;
    }

    LOG.error("Unregisterd class type" + other.getClass().getName());
  }

  public void plusBy(DenseDoubleVector other) {
    int rowId = other.getRowId();
    double[] values = other.getValues();

    if (this.values[rowId] == null) {
      this.values[rowId] = new Int2DoubleOpenHashMap();
    }

    for (int i = 0; i < values.length; i++) {
      if (values[i] != 0) {
        this.values[rowId].addTo(i, values[i]);
      }
    }
  }

  public void plusBy(SparseDoubleVector other) {
    inc(other.getRowId(), other.getIndices(), other.getValues());
  }

  @Override
  public double get(int rowId, int colId) {
    if (values[rowId] == null)
      return 0;
    return values[rowId].get(colId);
  }

  /**
   * get the vector of matrix
   * 
   * @param rowId the row id
   * @return
   */
  @Override
  public TDoubleVector getTDoubleVector(int rowId) {
    if (values[rowId] == null)
      return null;
    SparseDoubleVector vector = new SparseDoubleVector(col, values[rowId]);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    return vector;
  }

  /**
   * get sparsity of matrix
   * 
   * @return
   */
  @Override
  public double sparsity() {
    return (double) nonZeroNum() / (row * col);
  }

  /**
   * get size of matrix
   *
   * @return
     */
  @Override
  public int size() {
    return nonZeroNum();
  }

  /**
   * clear the matrix
   *
   */
  @Override
  public void clear() {
    Arrays.fill(values, null);
  }


  @Override
  public int nonZeroNum() {
    int sum = 0;
    for (int i = 0; i < row; i++) {
      if (values[i] != null) {
        sum += values[i].size();
      }
    }
    return sum;
  }
}
