package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.CompSparseFloatVector;

/**
 * Sparse double matrix that is represented by a group of component sparse float vector  {@link CompSparseFloatVector}
 */
public class CompSparseFloatMatrix extends TFloatMatrix {
  /**
   * Create a ComponentSparseFloatMatrix
   *
   * @param row the row
   * @param col the col
   */
  public CompSparseFloatMatrix(int row, int col) {
    super(row, col);
  }

  @Override public TVector initVector(int rowIndex) {
    return new CompSparseFloatVector(matrixId, rowIndex, col);
  }
}
