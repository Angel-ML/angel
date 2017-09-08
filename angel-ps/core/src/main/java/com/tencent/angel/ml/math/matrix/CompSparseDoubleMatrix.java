package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.CompSparseDoubleVector;

/**
 * Sparse double matrix that is represented by a group of component sparse double vector {@link CompSparseDoubleVector}
 */
public class CompSparseDoubleMatrix extends TDoubleMatrix {
  /**
   * Create a ComponentSparseDoubleMatrix
   *
   * @param row row number
   * @param col row vector dimension
   */
  public CompSparseDoubleMatrix(int row, int col) {
    super(row, col);
  }

  @Override public TVector initVector(int rowIndex) {
    return  new CompSparseDoubleVector(matrixId, rowIndex, col);
  }
}
