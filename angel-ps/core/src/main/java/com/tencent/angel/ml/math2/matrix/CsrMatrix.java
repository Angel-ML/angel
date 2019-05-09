package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.vector.Vector;

public abstract class CsrMatrix extends Matrix{
  protected int [] indices;
  protected int [] indptr;
  protected int [] shape;

  public CsrMatrix() {}

  public abstract Vector getRow(int idx);

  public abstract Vector getCol(int idx);

  public int[] getShape() {
    return shape;
  }

  @Override
  public int getNumRows() {
    return shape[0];
  }

  public int getNumCols() {
    return shape[1];
  }

  public int[] getIndices() {
    return indices;
  }

  public int[] getIndptr() {
    return indptr;
  }

  public void reshape(int rows, int cols) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  protected int [] trans(int[] rowIndices){
    int start = 0; int end = 0;
    int [] indptr = new int[shape[0] + 1];
    indptr[0] = 0;
    int j = 1;
    for (int i = 0; i < rowIndices.length; i++) {
      if (start != rowIndices[i]) {
        indptr[j] = end;
        j++;
      }
      start = rowIndices[i];
      end = end + 1;
    }
    indptr[shape[0]] = end;
    return indptr;
  }

  @Override
  public Vector diag() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector dot(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector transDot(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iadd(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix add(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix isub(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix sub(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix imul(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix mul(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix idiv(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix div(int rowId, Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iaxpy(int rowId, Vector other, double aplha) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix axpy(int rowId, Vector other, double aplha) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iadd(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix add(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix isub(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix sub(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix imul(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix mul(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix idiv(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix div(Vector other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iaxpy(Vector other, double aplha) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix axpy(Vector other, double aplha) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iadd(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix add(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix isub(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix sub(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix imul(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix mul(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix idiv(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix div(Matrix other) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iaxpy(Matrix other, double aplha) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix axpy(Matrix other, double aplha) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix iadd(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix add(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix isub(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix sub(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix imul(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix mul(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix idiv(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix div(double x) {
    throw new UnsupportedOperationException("this operation is not support!");
  }



  @Override
  public Vector min(int axis) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector max(int axis) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector sum(int axis) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector average(int axis) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector std(int axis) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector norm(int axis) {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Matrix copy() {
    throw new UnsupportedOperationException("this operation is not support!");
  }
}
