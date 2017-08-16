package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.SparseFloatVector;

/**
 * Sparse float matrix that is represented by a group of sparse float vector {@link SparseFloatVector}
 */
public class SparseFloatMatrix extends TFloatMatrix{
  /**
   * Create a new float matrix.
   *
   * @param row the row
   * @param col the col
   */
  public SparseFloatMatrix(int row, int col) {
    super(row, col);
  }

  @Override public TVector initVector(int rowIndex) {
    SparseFloatVector ret = new SparseFloatVector(col);
    ret.setMatrixId(matrixId);
    ret.setRowId(rowIndex);
    return ret;
  }
}
