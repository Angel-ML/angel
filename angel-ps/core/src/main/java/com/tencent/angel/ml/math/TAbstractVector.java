/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.math;

import com.tencent.angel.ml.matrix.RowType;

/**
 * Vector is the Class operated by users.
 */
public abstract class TAbstractVector extends TUpdate {

  /**
   * The Row id.
   */
  protected int rowId;
  /**
   * The Matrix id.
   */
  protected int matrixId;
  /**
   * The Clock.
   */
  protected int clock;
  /**
   * The Dimension.
   */
  protected int dim;

  /**
   * Default construction.
   */
  public TAbstractVector() {
    this.rowId = -1;
    this.matrixId = -1;
    this.clock = -1;
    this.dim = -1;
  }

  /**
   * Create a new vector by other.
   *
   * @param other the other
   */
  public TAbstractVector(TAbstractVector other) {
    this.rowId = other.getRowId();
    this.matrixId = other.getMatrixId();
    this.clock = other.getClock();
    this.dim = other.getDimension();
  }

  /**
   * Gets sparsity factor.
   *
   * @return Return the sparsity of vector
   */
  public abstract double sparsity();

  /**
   * Gets type of Vector.
   *
   * @return the type
   */
  public abstract RowType getType();

  /**
   * Gets dimension,the represented length of vector
   *
   * @return the dimension
   */
  public int getDimension() {
    return dim;
  }

  /**
   * Gets row id.
   *
   * @return the row id
   */
  public int getRowId() {
    return rowId;
  }

  @Override
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Gets clock.
   *
   * @return the clock
   */
  public int getClock() {
    return clock;
  }

  /**
   * Sets row id.
   *
   * @param rowId the row id
   * @return the row id
   */
  public TAbstractVector setRowId(int rowId) {
    this.rowId = rowId;
    return this;
  }

  @Override
  public TAbstractVector setMatrixId(int matrixId) {
    this.matrixId = matrixId;
    return this;
  }

  /**
   * Sets clock.
   *
   * @param clock the clock
   * @return the clock
   */
  public TAbstractVector setClock(int clock) {
    this.clock = clock;
    return this;
  }

  /**
   * The actually value size.
   *
   *
   * @return if sparse then return size of non-zero values, else size of all
   */
  public abstract int size();
}
