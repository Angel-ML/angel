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


package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.ufuncs.executor.matrix.BinaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.DotMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.UnaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.*;
import com.tencent.angel.ml.math2.vector.*;

public abstract class BlasMatrix extends Matrix {
  protected int numRows, numCols;

  public BlasMatrix() {
  }

  public abstract Vector getRow(int idx);

  public abstract Vector getCol(int idx);

  @Override public int getNumRows() {
    return numRows;
  }

  public void setNumRows(int numRows) {
    this.numRows = numRows;
  }

  public int getNumCols() {
    return numCols;
  }

  public void setNumCols(int numCols) {
    this.numCols = numCols;
  }

  public void reshape(int rows, int cols) {
    assert rows * cols == numRows * numCols;
    numRows = rows;
    numCols = cols;
  }

  public Matrix iadd(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Add(true));
  }

  public Matrix add(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Add(false));
  }

  public Matrix isub(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Sub(true));
  }

  public Matrix sub(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Sub(false));
  }

  public Matrix imul(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Mul(true));
  }

  public Matrix mul(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Mul(false));
  }

  public Matrix idiv(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Div(true));
  }

  public Matrix div(int rowId, Vector other) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Div(false));
  }

  public Matrix iaxpy(int rowId, Vector other, double aplha) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Axpy(true, aplha));
  }

  public Matrix axpy(int rowId, Vector other, double aplha) {
    return BinaryMatrixExecutor.apply(this, other, rowId, false, new Axpy(false, aplha));
  }

  public Matrix iadd(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Add(true));
  }

  public Matrix add(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Add(false));
  }

  public Matrix isub(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Sub(true));
  }

  public Matrix sub(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Sub(false));
  }

  public Matrix imul(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Mul(true));
  }

  public Matrix mul(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Mul(false));
  }

  public Matrix idiv(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Div(true));
  }

  public Matrix div(Vector other) {
    return BinaryMatrixExecutor.apply(this, other, false, new Div(false));
  }

  public Matrix iaxpy(Vector other, double aplha) {
    return BinaryMatrixExecutor.apply(this, other, false, new Axpy(true, aplha));
  }

  public Matrix axpy(Vector other, double aplha) {
    return BinaryMatrixExecutor.apply(this, other, false, new Axpy(false, aplha));
  }

  public Matrix iadd(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Add(true));
  }

  public Matrix add(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Add(false));
  }

  public Matrix isub(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Sub(true));
  }

  public Matrix sub(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Sub(false));
  }

  public Matrix imul(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Mul(true));
  }

  public Matrix mul(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Mul(false));
  }

  public Matrix idiv(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Div(true));
  }

  public Matrix div(Matrix other) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Div(false));
  }

  public Matrix iaxpy(Matrix other, double aplha) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Axpy(true, aplha));
  }

  public Matrix axpy(Matrix other, double aplha) {
    return BinaryMatrixExecutor.apply(this, false, other, false, new Axpy(false, aplha));
  }

  public Matrix iadd(double x) {
    return UnaryMatrixExecutor.apply(this, new SAdd(true, x));
  }

  public Matrix add(double x) {
    return UnaryMatrixExecutor.apply(this, new SAdd(false, x));
  }

  public Matrix isub(double x) {
    return UnaryMatrixExecutor.apply(this, new SSub(true, x));
  }

  public Matrix sub(double x) {
    return UnaryMatrixExecutor.apply(this, new SSub(false, x));
  }

  public Matrix imul(double x) {
    return UnaryMatrixExecutor.apply(this, new SMul(true, x));
  }

  public Matrix mul(double x) {
    return UnaryMatrixExecutor.apply(this, new SMul(false, x));
  }

  public Matrix idiv(double x) {
    return UnaryMatrixExecutor.apply(this, new SDiv(true, x));
  }

  public Matrix div(double x) {
    return UnaryMatrixExecutor.apply(this, new SDiv(false, x));
  }

  public Vector dot(Vector v) {
    return DotMatrixExecutor.apply(this, false, v);
  }

  public Vector transDot(Vector v) {
    return DotMatrixExecutor.apply(this, true, v);
  }

  public Matrix dot(Matrix m) {
    return DotMatrixExecutor.apply(this, false, m, false);
  }
}
