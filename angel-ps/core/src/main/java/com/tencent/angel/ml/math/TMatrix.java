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

package com.tencent.angel.ml.math;

/**
 * The base matrix,represented a multidimensional values.
 */
public abstract class TMatrix extends TUpdate {
  /**
   * The Matrix id.
   */
  protected int matrixId;
  /**
   * The Row.
   */
  protected int row;
  /**
   * The Col.
   */
  protected int col;

  /**
   * Instantiates a new matrix.
   */
  public TMatrix() {
    this.matrixId = -1;
    this.row = -1;
    this.col = -1;
  }

  /**
   * Instantiates a new matrix.
   *
   * @param row the row
   * @param col the col
   */
  public TMatrix(int row, int col) {
    this.matrixId = -1;
    this.row = row;
    this.col = col;
  }

  @Override
  public TMatrix setMatrixId(int matrixId) {
    this.matrixId = matrixId;
    return this;
  }

  /**
   * Gets row num.
   *
   * @return the row num
   */
  public int getRowNum() {
    return row;
  }

  /**
   * Gets col num.
   *
   * @return the col num
   */
  public int getColNum() {
    return col;
  }

  @Override
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Gets sparsity factor.
   *
   * @return the sparsity
   */
  public abstract double sparsity();

  /**
   * Size of matrix,normally <code>rows * columns</code>.
   *
   * @return the size
   */
  public abstract long size();

  /**
   * Clear.
   */
  public abstract void clear();

  /**
   * Clear.
   */
  public abstract void clear(int rowIndex);

  /**
   * Gets non zero num of matrix
   *
   * @return the result
   */
  public abstract long nonZeroNum();



  /**
   * Plus by other matrix.
   *
   * @param other the other matrix
   * @return this
   */
  public abstract TMatrix plusBy(TMatrix other);

}
