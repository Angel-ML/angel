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

import com.tencent.angel.ml.math.vector.TIntVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Arrays;

/**
 * Row-based linked list sparse matrix format
 */
public class LILIntMatrix extends TIntMatrix {

  private static final Log LOG = LogFactory.getLog(LILIntMatrix.class);

  Int2IntOpenHashMap[] values;

  /**
   * Create a new empty matrix.
   *
   * @param row the row
   * @param col the col
   */
  public LILIntMatrix(int row, int col) {
    super(row, col);
    values = new Int2IntOpenHashMap[row];
  }

  @Override
  public void inc(int rowId, int colId, int value) {
    if (values[rowId] == null) {
      values[rowId] = new Int2IntOpenHashMap();
    }

    int old = values[rowId].get(colId);
    values[rowId].put(colId, old + value);
  }

  /**
   * Increases specified elements row by <code>values</code>.
   *
   * @param rowIds the row ids
   * @param colIds the col ids
   * @param values the values
   */
  public void inc(int[] rowIds, int[] colIds, int[] values) {
    inc(rowIds, colIds, values, 0, rowIds.length);
  }

  /**
   * Increases specified elements by values restrictively.
   *
   * @param rowIds the row ids
   * @param colIds the col ids
   * @param values the values
   * @param start the start
   * @param end the end
   */
  public void inc(int[] rowIds, int[] colIds, int[] values, int start, int end) {
    for (int i = start; i < end; i++) {
      int rowId = rowIds[i];
      int colId = colIds[i];
      int value = values[i];

      if (this.values[rowId] == null) {
        this.values[rowId] = new Int2IntOpenHashMap();
      }

      int old = this.values[rowId].get(colId);
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
  public void inc(int rowId, int[] colIds, int[] values) {
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
  public void inc(int rowId, int[] colIds, int[] values, int start, int end) {
    if (this.values[rowId] == null) {
      this.values[rowId] = new Int2IntOpenHashMap();
    }

    for (int i = start; i < end; i++) {
      int colId = colIds[i];
      int value = values[i];

      int old = this.values[rowId].get(colId);
      this.values[rowId].put(colId, old + value);
    }
  }

  /**
   * plus the matrix
   * 
   * @param other the other
   */
  @Override
  public void plusBy(TIntMatrix other) {
    if (other instanceof COOIntMatrix) {
      plusBy((COOIntMatrix) other);
      return;
    }

    LOG.error(String.format("Unregistered class type %s", other.getClass().getName()));
  }

  public void plusBy(COOIntMatrix other) {
    inc(other.rowIds, other.colIds, other.values, 0, other.size);
  }

  @Override
  public void plusBy(TIntVector other) {
    if (other instanceof SparseIntVector) {
      plusBy((SparseIntVector) other);
      return;
    }

    if (other instanceof DenseIntVector) {
      plusBy((DenseIntVector) other);
      return;
    }


    LOG.error("Unregisterd class type " + other.getClass().getName());
  }

  public void plusBy(SparseIntVector other) {
    inc(other.getRowId(), other.getIndices(), other.getValues());
  }

  public void plusBy(DenseIntVector other) {
    int rowId = other.getRowId();
    int[] values = other.getValues();

    if (this.values[rowId] == null) {
      this.values[rowId] = new Int2IntOpenHashMap();
    }

    for (int i = 0; i < values.length; i++) {
      if (values[i] != 0) {
        this.values[rowId].addTo(i, values[i]);
      }
    }
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
    return nonZeroNum();
  }

  /**
   * get the value of one element
   * 
   * @param rowId the row id
   * @param colId the col id
   * @return
   */
  @Override
  public int get(int rowId, int colId) {
    if (values[rowId] == null)
      return 0;
    return values[rowId].get(colId);
  }

  /**
   * get vector of matrix
   * 
   * @param rowId the row id
   * @return
   */
  @Override
  public TIntVector getTIntVector(int rowId) {
    if (values[rowId] == null)
      return null;
    SparseIntVector vector = new SparseIntVector(col, values[rowId]);
    vector.setMatrixId(matrixId);
    vector.setRowId(rowId);
    return vector;
  }

  /**
   * clear the matrix
   * 
   */
  @Override
  public void clear() {
    Arrays.fill(values, null);
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
      if (values[i] != null) {
        sum += values[i].size();
      }
    }
    return sum;
  }
}
