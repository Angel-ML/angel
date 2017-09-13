package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.CompSparseDoubleLongKeyVector;

/**
 * Sparse double matrix that is represented by a group of component sparse double long key vector  {@link CompSparseDoubleLongKeyVector}
 */
public class CompSparseDoubleLongKeyMatrix extends DoubleLongKeyMatrix {
  /**
   * Create a CompSparseDoubleLongKeyMatrix
   *
   * @param row row number
   * @param col row vector dimension
   */
  public CompSparseDoubleLongKeyMatrix(int row, long col) {
    super(row, col);
  }

  @Override public TVector initVector(int rowIndex) {
    return new CompSparseDoubleLongKeyVector(matrixId, rowIndex, col);
  }
}
