package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.Vector;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class CooDoubleMatrix extends CooMatrix{

  protected double [] values;

  public CooDoubleMatrix() {
    super();
  }

  public CooDoubleMatrix(int matrixId, int clock, int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.rowIndices = rowIndices;
    this.colIndices = colIndices;
    this.values = values;
    this.shape = shape;
  }

  public CooDoubleMatrix(int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    this(0, 0, rowIndices, colIndices, values, shape);
  }

  @Override
  public Vector getRow(int idx) {
    IntArrayList cols = new IntArrayList();
    DoubleArrayList data = new DoubleArrayList();
    for (int i = 0; i < rowIndices.length; i++) {
      if (rowIndices[i] == idx) {
        cols.add(colIndices[i]);
        data.add(values[i]);
      }
    }

    IntDoubleSparseVectorStorage storage = new IntDoubleSparseVectorStorage(shape[1], cols.toIntArray(), data.toDoubleArray());
    return new IntDoubleVector(getMatrixId(), idx, getClock(), shape[1], storage);
  }


  @Override
  public Vector getCol(int idx) {
    IntArrayList cols = new IntArrayList();
    DoubleArrayList data = new DoubleArrayList();
    for (int i = 0; i < colIndices.length; i++) {
      if (colIndices[i] == idx) {
        cols.add(rowIndices[i]);
        data.add(values[i]);
      }
    }

    IntDoubleSparseVectorStorage storage = new IntDoubleSparseVectorStorage(shape[0], cols.toIntArray(), data.toDoubleArray());
    return new IntDoubleVector(getMatrixId(), 0, getClock(), shape[0], storage);
  }

  public double[] getValues() {
    return values;
  }

  @Override
  public void clear() {
    matrixId = 0;
    clock = 0;
    rowIndices = null;
    colIndices = null;
    values = null;
  }

  @Override
  public Matrix copy() {
    double[] newData = new double[values.length];
    System.arraycopy(values, 0, newData, 0, values.length);
    return new CooDoubleMatrix(matrixId, clock, rowIndices, colIndices, newData, shape);
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
