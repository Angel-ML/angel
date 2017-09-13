package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.CompSparseIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Sparse double matrix that is represented by a group of component sparse int vector  {@link CompSparseIntVector}
 */
public class CompSparseIntMatrix extends TIntMatrix {
  private static final Log LOG = LogFactory.getLog(SparseIntMatrix.class);

  /**
   * Create a ComponentSparseIntMatrix
   *
   * @param row the row number
   * @param col the col number
   */
  public CompSparseIntMatrix(int row, int col) {
    super(row, col);
  }

  /**
   * init the empty vector
   *
   * @param rowIndex row index
   * @return
   */
  @Override public TVector initVector(int rowIndex) {
    return new CompSparseIntVector(matrixId, rowIndex, (int)col);
  }
}
