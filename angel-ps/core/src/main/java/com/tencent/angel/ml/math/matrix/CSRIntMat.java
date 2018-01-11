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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TMatrix;

/**
 * The compressed sparse row of matrix.
 */
public class CSRIntMat extends TMatrix {

  public int[] rs;
  public int[] cols;
  public int[] vals;

  /**
   * Create a new empty matrix.
   *
   * @param row the row
   * @param col the col
   */
  public CSRIntMat(int row, int col) {
    super(row, col);
  }

  /**
   * Create a new matrix with <code>vals</code>.
   *
   * @param row  the row
   * @param col  the col
   * @param rs   the rs
   * @param cols the cols
   * @param vals the vals
   */
  public CSRIntMat(int row, int col, int[] rs, int[] cols, int[] vals) {
    this(row, col);
    this.rs = rs;
    this.cols = cols;
    this.vals = vals;
  }

  @Override public double sparsity() {
    return 0;
  }

  @Override public long size() {
    return 0;
  }

  @Override public void clear() {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public void clear(int rowIndex) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  @Override public long nonZeroNum() {
    return 0;
  }

  @Override public TMatrix plusBy(TMatrix other) {
    throw new UnsupportedOperationException("Unsupport operation");
  }

  //TODO Should be implemented
  @Override public Object getRow(int rowIndex) {
    return null;
  }

  /**
   * convert to COOIntMatrix
   *
   * @return
   */
  public COOIntMatrix toCoo() {
    int len = cols.length;
    int[] rr = new int[len];
    int[] cc = new int[len];
    int[] vv = new int[len];

    for (int r = 0; r < rs.length - 1; r++) {
      for (int j = rs[r]; j < rs[r + 1]; j++) {
        rr[j] = r;
        cc[j] = cols[j];
        vv[j] = vals[j];
      }
    }

    return new COOIntMatrix(row, (int)col, rr, cc, vv);
  }

  /**
   * information about the matrix
   *
   * @return String
   */
  @Override public String toString() {
    StringBuilder builder = new StringBuilder();
    for (int r = 0; r < rs.length - 1; r++) {
      for (int j = rs[r]; j < rs[r + 1]; j++) {
        builder.append(String.format("%d %d %d\n", r, cols[j], vals[j]));
      }
    }
    return builder.toString();
  }

  /**
   * information about the matrix by r index
   *
   * @param r
   * @return
   */
  public String str(int r) {
    StringBuilder builder = new StringBuilder();
    for (int j = rs[r]; j < rs[r + 1]; j++) {
      builder.append(String.format("%d %d %d\n", r, cols[j], vals[j]));
    }
    return builder.toString();
  }
}
