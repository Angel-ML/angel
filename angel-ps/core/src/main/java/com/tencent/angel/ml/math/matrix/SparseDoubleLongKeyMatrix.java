package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.LongKeySparseDoubleVector;

/**
 * Sparse double matrix that is represented by a group of sparse double vector {@link LongKeySparseDoubleVector}
 */
public class SparseDoubleLongKeyMatrix extends LongKeyDoubleMatrix {

  /**
   * Create a SparseDoubleLongKeyMatrix
   * @param row row number
   * @param col row vector dimension
   */
  public SparseDoubleLongKeyMatrix(int row, long col) {
    super(row, col);
  }

  @Override public TVector initVector(int rowIndex) {
    LongKeySparseDoubleVector ret = new LongKeySparseDoubleVector(columnNum);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }
}
