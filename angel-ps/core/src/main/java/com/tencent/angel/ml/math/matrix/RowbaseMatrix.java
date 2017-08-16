package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;

import java.util.Arrays;

/**
 * The base class of Matrix that defined by a row vector list
 */
public abstract class RowbaseMatrix extends TMatrix {
  /* Row vectors of matrix */
  protected final TVector[] vectors;

  /**
   * Build a RowbaseMatrix
   * @param row row number
   * @param col column number
   */
  public RowbaseMatrix(int row, int col) {
    super(row, col);
    vectors = new TVector[row];
  }

  /**
   * Build a RowbaseMatrix
   * @param row row number
   */
  public RowbaseMatrix(int row) {
    this(row, -1);
  }

  @Override
  public double sparsity() {
    return ((double) nonZeroNum()) / (row * col);
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
  public TVector getTVector(int rowIndex) {
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
        plusBy(((RowbaseMatrix)other).getTVector(i));
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
  public abstract TVector initVector(int rowIndex);
}
