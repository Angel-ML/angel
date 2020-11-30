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

import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.Vector;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;

public class CsrFloatMatrix extends CsrMatrix {

  protected float[] values;

  public CsrFloatMatrix() {
    super();
  }

  public CsrFloatMatrix(int matrixId, int clock, int[] rowIndices, int[] colIndices, float[] values, int[] shape) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.shape = shape;
    this.indptr = trans(rowIndices);
    this.indices = colIndices;
    this.values = values;
  }

  public CsrFloatMatrix(int[] rowIndices, int[] colIndices, float[] values, int[] shape) {
    this(0, 0, rowIndices, colIndices, values, shape);
  }

  public CsrFloatMatrix(int matrixId, int clock, float[] values, int[] indices, int[] indptr, int[] shape) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.values = values;
    this.shape = shape;
    this.indices = indices;
    this.indptr = indptr;
  }

  public CsrFloatMatrix(float[] values, int[] indices, int[] indptr, int[] shape) {
    this(0, 0, values, indices, indptr, shape);
  }

  @Override
  public Vector getRow(int idx) {
    IntArrayList cols = new IntArrayList();
    FloatArrayList data = new FloatArrayList();

    int rowNum = indptr.length - 1;
    assert (idx < rowNum);
    for (int i = indptr[idx]; i < indptr[idx + 1]; i++) {
      cols.add(indices[i]);
      data.add(values[i]);
    }

    IntFloatSparseVectorStorage storage =
        new IntFloatSparseVectorStorage(shape[1], cols.toIntArray(), data.toFloatArray());
    return new IntFloatVector(getMatrixId(), idx, getClock(), shape[1], storage);
  }

  @Override
  public Vector getCol(int idx) {
    IntArrayList cols = new IntArrayList();
    FloatArrayList data = new FloatArrayList();
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


    IntFloatSparseVectorStorage storage =
        new IntFloatSparseVectorStorage(shape[0], cols.toIntArray(), data.toFloatArray());
    return new IntFloatVector(getMatrixId(), 0, getClock(), shape[0], storage);
  }

  public float[] getValues() {
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
    float[] newData = new float[values.length];
    System.arraycopy(values, 0, newData, 0, values.length);
    return new CsrFloatMatrix(matrixId, clock, newData, indices, indptr, shape);
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
