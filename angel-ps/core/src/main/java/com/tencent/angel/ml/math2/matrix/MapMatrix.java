package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.vector.Vector;
import java.util.HashMap;

public class MapMatrix<Vec extends Vector> extends Matrix {

  protected HashMap<Long, Vec> mapMatrix;

  public MapMatrix() {
  }

  public MapMatrix(int matrixId, int clock, HashMap<Long, Vec> mapMatrix) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.mapMatrix = mapMatrix;
  }

  public MapMatrix(HashMap<Long, Vec> mapMatrix) {
    this(0, 0, mapMatrix);
  }

  public Vector getRow(int idx) {
    return mapMatrix.get(idx);
  }

  public Vector getRow(long idx) {
    return mapMatrix.get(idx);
  }

  public HashMap<Long, Vec> getRows(int[] idx) {
    HashMap<Long, Vec> matrix = new HashMap();
    for (int id : idx) {
      matrix.put((long) id, mapMatrix.get(id));
    }
    return matrix;
  }

  public HashMap<Long, Vec> getRows(long[] idx) {
    HashMap<Long, Vec> matrix = new HashMap();
    for (long id : idx) {
      matrix.put(id, mapMatrix.get(id));
    }
    return matrix;
  }

  public HashMap<Long, Vec> getRows() {
    return mapMatrix;
  }

  public void setRows(HashMap<Long, Vec> matrix) {
    mapMatrix.putAll(matrix);
  }

  public void setRow(int idx, Vec v) {
    mapMatrix.put((long) idx, v);
  }

  public void setRow(long idx, Vec v) {
    mapMatrix.put(idx, v);
  }

  public void setRows(int[] idx, Vec[] vectors) {
    for (int i = 0; i < idx.length; i++) {
      mapMatrix.put((long) idx[i], vectors[i]);
    }
  }

  public void setRows(long[] idx, Vec[] vectors) {
    for (int i = 0; i < idx.length; i++) {
      mapMatrix.put(idx[i], vectors[i]);
    }
  }

  @Override
  public Vector diag() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public Vector getCol(int idx) {
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
  public double min() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public double max() {
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

  @Override
  public int getNumRows() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public void clear() {
    mapMatrix.clear();
  }

  @Override
  public double sum() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public double std() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public double average() {
    throw new UnsupportedOperationException("this operation is not support!");
  }

  @Override
  public double norm() {
    throw new UnsupportedOperationException("this operation is not support!");
  }
}
