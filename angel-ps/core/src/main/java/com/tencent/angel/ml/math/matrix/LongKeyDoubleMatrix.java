package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.vector.LongKeyDoubleVector;

/**
 * Base class of double matrix with long key row vector.
 */
public abstract class LongKeyDoubleMatrix extends RowbaseMatrix {
  /** Dimension of row vector */
  protected final long columnNum;

  /**
   * Create a LongKeyDoubleMatrix
   * @param row row number
   * @param col row vector dimension
   */
  public LongKeyDoubleMatrix(int row, long col) {
    super(row, -1);
    this.columnNum = col;
  }

  @Override
  public int getColNum() {
    throw new UnsupportedOperationException("Unsupportted operation, please use getColumnNum instead");
  }

  public long getColumnNum(){
    return columnNum;
  }

  /**
   * Plus specified element of matrix by a update value.
   *
   * @param rowIndex the row index
   * @param colIndex the column index
   * @param value the value update value
   * @return this
   */
  public LongKeyDoubleMatrix plusBy(int rowIndex, long colIndex, double value){
    if (vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }
    ((LongKeyDoubleVector)vectors[rowIndex]).plusBy(colIndex, value);
    return this;
  }

  /**
   * Increases specified elements by values.
   *
   * @param rowIndexes the row ids
   * @param colIndexes the col ids
   * @param values the values
   * @return this
   */
  public LongKeyDoubleMatrix plusBy(int[] rowIndexes, long[] colIndexes, double[] values) {
    assert ((rowIndexes.length == colIndexes.length) && (rowIndexes.length == values.length));
    for(int i = 0; i < rowIndexes.length; i++) {
      if(vectors[rowIndexes[i]] == null) {
        vectors[rowIndexes[i]] = initVector(rowIndexes[i]);
      }
      ((LongKeyDoubleVector)vectors[rowIndexes[i]]).plusBy(colIndexes[i], values[i]);
    }
    return this;
  }

  /**
   * Increases specified row by values.
   *
   * @param rowIndex the row id
   * @param colIndexes the col ids
   * @param values the values
   * @return this
   */
  public LongKeyDoubleMatrix plusBy(int rowIndex, long[] colIndexes, double[] values) {
    assert (colIndexes.length == values.length);
    if(vectors[rowIndex] == null) {
      vectors[rowIndex] = initVector(rowIndex);
    }

    for(int i = 0; i < colIndexes.length; i++) {
      ((LongKeyDoubleVector)vectors[rowIndex]).plusBy(colIndexes[i], values[i]);
    }
    return this;
  }

  /**
   * Get specified value.
   *
   * @param rowIndex the row index
   * @param colIndex the column index
   * @return the value
   */
  public double get(int rowIndex, long colIndex) {
    if(vectors[rowIndex] == null) {
      return 0.0;
    }
    return ((LongKeyDoubleVector)vectors[rowIndex]).get(colIndex);
  }
}
