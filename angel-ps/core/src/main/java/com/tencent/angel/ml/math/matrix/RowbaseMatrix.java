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

import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;

import java.util.Arrays;

/**
 * The base class of Matrix that defined by a row vector list
 */
public abstract class RowbaseMatrix<ROW extends TVector> extends TMatrix<ROW> {
  /* Row vectors of matrix */
  protected ROW[] vectors;

  /**
   * Build a RowbaseMatrix
   * @param row row number
   * @param col column number
   */
  public RowbaseMatrix(int row, long col, ROW[] vectors) {
    super(row, col);
    this.vectors = vectors;
  }

  public ROW[] getVectors() { return vectors; }

  @Override
  public double sparsity() {
    if(col <= 0) {
      return ((double) nonZeroNum()) / row / Long.MAX_VALUE / 2;
    } else {
      return ((double) nonZeroNum()) / (row * col);
    }
  }

  @Override
  public long size() {
    return row * col;
  }

  @Override
  public void clear() {
    Arrays.fill(vectors, null);
  }

  @Override
  public void clear(int rowIndex) {
    vectors[rowIndex] = null;
  }

  @Override
  public long nonZeroNum() {
    long num = 0;
    for(int i = 0; i < row; i++) {
      if(vectors[i] != null) {
        num += vectors[i].nonZeroNumber();
      }
    }
    return num;
  }

  /**
   * Gets specified vector.
   *
   * @param rowIndex the row id
   * @return the vector if exists
   */
  @Override
  public ROW getRow(int rowIndex) {
    if(vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }
    return vectors[rowIndex];
  }

  @Override
  public TMatrix plusBy(TMatrix other) {
    if (other == null) {
      return this;
    }

    assert (row == other.getRowNum());
    assert (col == other.getColNum());
    if(other instanceof RowbaseMatrix) {
      for (int i = 0; i < row; i++) {
        plusBy(((RowbaseMatrix)other).getRow(i));
      }
      return this;
    } else {
      throw new UnsupportedOperationException("Do not support " + this.getClass().getName()
        + " plusBy " + other.getClass().getName());
    }
  }

  /**
   * Plus a row vector of matrix use a vector with same dimension
   * @param other update vector
   */
  public TMatrix plusBy(TVector other) {
    if(other == null) {
      return this;
    }

    int rowIndex = other.getRowId();
    if(vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }

    vectors[rowIndex].plusBy(other);
    return this;
  }

  /**
   * Init a row vector of the matrix
   * @param rowIndex row index
   * @return row vector
   */
  public abstract ROW initVector(int rowIndex);

  public void setRow(int rowIndex, ROW vect) {
    vect.setMatrixId(matrixId);
    vect.setRowId(rowIndex);
    vectors[rowIndex] = vect;
  }
}
