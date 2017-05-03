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

/**
 * The coordinate int matrix
 */
public class COOIntMatrix extends TIntMatrix {

  private static final int INIT_SIZE = 256;

  int[] rowIds;
  int[] colIds;
  int[] values;

  int size;

  /**
   * Create a new empty matrix.
   *
   * @param row the row
   * @param col the col
   */
  public COOIntMatrix(int row, int col) {
    this(row, col, INIT_SIZE);
  }

  /**
   * Create a new empty int matrix with specified <code>capacity</code>.
   *
   * @param row the row
   * @param col the col
   * @param capacity the capacity
   */
  public COOIntMatrix(int row, int col, int capacity) {
    super(row, col);
    rowIds = new int[capacity];
    colIds = new int[capacity];
    values = new int[capacity];
    size = 0;
  }

  /**
   * Create a new int matrix with specified param.
   * 
   * @param row
   * @param col
   * @param rowIds
   * @param colIds
   * @param values
   */
  public COOIntMatrix(int row, int col, int[] rowIds, int[] colIds, int[] values) {
    this(row, col, rowIds, colIds, values, 0);
  }

  public COOIntMatrix(int row, int col, int[] rowIds, int[] colIds, int[] values, int size) {
    super(row, col);
    this.rowIds = rowIds;
    this.colIds = colIds;
    this.values = values;
    this.size = size;
  }

  /**
   * inc the matrix by set rowId , colId , value
   * 
   * @param rowId the row id
   * @param colId the col id
   * @param value the value
   */
  @Override
  public void inc(int rowId, int colId, int value) {
    rowIds[size] = rowId;
    colIds[size] = colId;
    values[size] = value;
    size++;
  }

  @Override
  public void plusBy(TIntMatrix other) {

  }

  @Override
  public void plusBy(TIntVector other) {

  }

  @Override
  public int get(int rowId, int colId) {
    return 0;
  }

  @Override
  public TIntVector getTIntVector(int rowId) {
    return null;
  }

  @Override
  public double sparsity() {
    return 0;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public void clear() {
    size = 0;
  }

  /**
   * length of the capacity
   * 
   * @return
   */
  public int capacity() {
    return rowIds.length;
  }

  @Override
  public int nonZeroNum() {
    return 0;
  }

  /**
   * convert to CSRIntMat
   * 
   * @return
   */
  public CSRIntMat toCsr() {

    int[] rc = new int[row];
    for (int i = 0; i < size; i++) {
      rc[rowIds[i]]++;
    }

    int[] rs = new int[row + 1];
    rs[0] = 0;
    for (int i = 0; i < row; i++)
      rs[i + 1] = rs[i] + rc[i];

    for (int i = 1; i < row; i++)
      rc[i] = rc[i] + rc[i - 1];

    int[] cols = new int[colIds.length];
    int[] vals = new int[values.length];

    for (int i = size - 1; i >= 0; i--) {
      int r = rowIds[i];
      int c = colIds[i];
      int v = values[i];
      int idx = --rc[r];
      cols[idx] = c;
      vals[idx] = v;
    }

    return new CSRIntMat(row, col, rs, cols, vals);
  }

  /**
   * information about the matrix
   * 
   * @return String
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();

    for (int i = 0; i < size; i++) {
      builder.append(String.format("%d %d %d\n", rowIds[i], colIds[i], values[i]));
    }

    return builder.toString();
  }
}
