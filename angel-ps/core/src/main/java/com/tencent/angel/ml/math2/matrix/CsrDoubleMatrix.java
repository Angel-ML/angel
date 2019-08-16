/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.ml.math2.matrix;

import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.Vector;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class CsrDoubleMatrix extends CsrMatrix {

  protected double[] values;

  public CsrDoubleMatrix() {
    super();
  }

  public CsrDoubleMatrix(int matrixId, int clock, int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.shape = shape;
    this.indptr = trans(rowIndices);
    this.indices = colIndices;
    this.values = values;
  }

  public CsrDoubleMatrix(int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    this(0, 0, rowIndices, colIndices, values, shape);
  }

  public CsrDoubleMatrix(int matrixId, int clock, double[] values, int[] indices, int[] indptr, int[] shape) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.values = values;
    this.indices = indices;
    this.indptr = indptr;
    this.shape = shape;
  }

  public CsrDoubleMatrix(double[] values, int[] indices, int[] indptr, int[] shape) {
    this(0, 0, values, indices, indptr, shape);
  }

  @Override
  public Vector getRow(int idx) {
    IntArrayList cols = new IntArrayList();
    DoubleArrayList data = new DoubleArrayList();
    int rowNum = indptr.length - 1;
    assert (idx < rowNum);

    for (int i = indptr[idx]; i < indptr[idx + 1]; i++) {
      cols.add(indices[i]);
      data.add(values[i]);
    }

    IntDoubleSparseVectorStorage storage =
        new IntDoubleSparseVectorStorage(shape[1], cols.toIntArray(), data.toDoubleArray());
    return new IntDoubleVector(getMatrixId(), idx, getClock(), shape[1], storage);
  }

  @Override
  public Vector getCol(int idx) {
    IntArrayList cols = new IntArrayList();
    DoubleArrayList data = new DoubleArrayList();
    int[] rows = new int[indices.length];
    int i = 0;
    int j = 0;

    while (i < indptr.length - 1 && j < indptr.length - 1) {
      int r = indptr[i + 1] - indptr[i];
      for (int p = j; p < j + r; p++) {
        rows[p] = i;
      }
      if (r != 0) {
        j++;
      }
      i++;
    }
    for (int id = 0; id < indices.length; id++) {
      if (indices[id] == idx) {
        cols.add(rows[id]);
        data.add(values[id]);
      }
    }

    IntDoubleSparseVectorStorage storage =
        new IntDoubleSparseVectorStorage(shape[0], cols.toIntArray(), data.toDoubleArray());
    return new IntDoubleVector(getMatrixId(), 0, getClock(), shape[0], storage);
  }

  public double[] getValues() {
    return values;
  }

  @Override
  public void clear() {
    matrixId = 0;
    clock = 0;
    indices = null;
    indptr = null;
    values = null;
  }

  @Override
  public Matrix copy() {
    double[] newData = new double[values.length];
    System.arraycopy(values, 0, newData, 0, values.length);
    return new CsrDoubleMatrix(matrixId, clock, newData, indices, indptr, shape);
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
