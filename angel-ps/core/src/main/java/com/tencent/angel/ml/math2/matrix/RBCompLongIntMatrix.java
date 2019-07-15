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

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.storage.IntIntDenseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage;
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.vector.CompLongIntVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.Vector;

public class RBCompLongIntMatrix extends RowBasedMatrix<CompLongIntVector> {

  private long subDim;

  public RBCompLongIntMatrix() {
    super();
  }

  public RBCompLongIntMatrix(int matrixId, int clock, CompLongIntVector[] rows) {
    super(matrixId, clock, rows[0].getDim(), rows);
    assert null != rows[0];
    subDim = rows[0].getSubDim();
  }

  public RBCompLongIntMatrix(CompLongIntVector[] rows) {
    this(0, 0, rows);
  }

  public RBCompLongIntMatrix(int matrixId, int clock, int numRows, long numCols, long subDim) {
    super(matrixId, clock, numCols, new CompLongIntVector[numRows]);
    this.subDim = subDim;
  }

  public RBCompLongIntMatrix(int numRows, long numCols, long subDim) {
    this(0, 0, numRows, numCols, subDim);
  }

  public long getSubDim() {
    return subDim;
  }

  public void setSubDim(long subDim) {
    this.subDim = subDim;
  }

  public double get(int i, long j) {
    if (null == rows[i]) {
      initEmpty(i);
      return 0;
    } else {
      return rows[i].get(j);
    }
  }

  public void set(int i, long j, int value) {
    if (null == rows[i]) {
      initEmpty(i);
    }
    rows[i].set(j, value);
  }

  @Override
  public Vector diag() {
    int[] resArr = new int[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (null == rows[i]) {
        resArr[i] = 0;
      } else {
        resArr[i] = rows[i].get(i);
      }
    }

    IntIntDenseVectorStorage storage = new IntIntDenseVectorStorage(resArr);
    return new IntIntVector(getMatrixId(), 0, getClock(), resArr.length, storage);
  }

  @Override
  public Vector dot(Vector other) {
    int[] resArr = new int[rows.length];
    for (int i = 0; i < rows.length; i++) {
      resArr[i] = (int) rows[i].dot(other);
    }
    IntIntDenseVectorStorage storage = new IntIntDenseVectorStorage(resArr);
    return new IntIntVector(matrixId, 0, clock, rows.length, storage);
  }

  @Override
  public RowBasedMatrix calulate(int rowId, Vector other, Binary op) {
    assert other != null;
    RBCompLongIntMatrix res;
    if (op.isInplace()) {
      res = this;
    } else {
      res = new RBCompLongIntMatrix(matrixId, clock, rows.length, (long) cols, subDim);
    }

    if (null == rows[rowId]) {
      initEmpty(rowId);
    }

    if (op.isInplace()) {
      BinaryExecutor.apply(rows[rowId], other, op);
    } else {
      for (int i = 0; i < rows.length; i++) {
        if (i == rowId) {
          res.setRow(rowId, (CompLongIntVector) BinaryExecutor.apply(rows[rowId], other, op));
        } else if (rows[i] != null) {
          res.setRow(rowId, rows[i].copy());
        }
      }
    }

    return res;
  }

  @Override
  public RowBasedMatrix calulate(Vector other, Binary op) {
    assert other != null;
    RBCompLongIntMatrix res;
    if (op.isInplace()) {
      res = this;
    } else {
      res = new RBCompLongIntMatrix(matrixId, clock, rows.length, (long) cols, subDim);
    }
    if (op.isInplace()) {
      for (int rowId = 0; rowId < rows.length; rowId++) {
        if (null == rows[rowId]) {
          initEmpty(rowId);
        }
        BinaryExecutor.apply(rows[rowId], other, op);
      }
    } else {
      for (int rowId = 0; rowId < rows.length; rowId++) {
        if (null == rows[rowId]) {
          initEmpty(rowId);
        }
        res.setRow(rowId, (CompLongIntVector) BinaryExecutor.apply(rows[rowId], other, op));
      }
    }
    return res;
  }

  @Override
  public RowBasedMatrix calulate(Matrix other, Binary op) {
    assert other instanceof RowBasedMatrix;

    if (op.isInplace()) {
      for (int i = 0; i < rows.length; i++) {
        if (null == rows[i]) {
          initEmpty(i);
        }
        if (null == ((RowBasedMatrix) other).rows[i]) {
          ((RowBasedMatrix) other).initEmpty(i);
        }
        BinaryExecutor.apply(rows[i], ((RowBasedMatrix) other).rows[i], op);
      }
      return this;
    } else {
      CompLongIntVector[] outRows = new CompLongIntVector[rows.length];
      for (int i = 0; i < rows.length; i++) {
        if (null == rows[i]) {
          initEmpty(i);
        }
        if (null == ((RowBasedMatrix) other).rows[i]) {
          ((RowBasedMatrix) other).initEmpty(i);
        }
        outRows[i] =
            (CompLongIntVector) BinaryExecutor.apply(rows[i], ((RowBasedMatrix) other).rows[i], op);
      }
      return new RBCompLongIntMatrix(matrixId, clock, outRows);
    }
  }

  @Override
  public RowBasedMatrix calulate(Unary op) {
    if (op.isInplace()) {
      for (Vector vec : rows) {
        UnaryExecutor.apply(vec, op);
      }
      return this;
    } else {
      CompLongIntVector[] outRows = new CompLongIntVector[rows.length];
      for (int i = 0; i < rows.length; i++) {
        if (null == rows[i]) {
          initEmpty(i);
        }
        outRows[i] = (CompLongIntVector) UnaryExecutor.apply(rows[i], op);
      }
      return new RBCompLongIntMatrix(matrixId, clock, outRows);
    }
  }

  @Override
  public void setRow(int idx, CompLongIntVector v) {
    assert cols == v.getDim();
    rows[idx] = v;
  }

  @Override
  public void setRows(CompLongIntVector[] rows) {
    for (CompLongIntVector v : rows) {
      assert cols == v.getDim();
    }
    this.rows = rows;
  }

  @Override
  public void initEmpty(int idx) {
    int numComp = (int) ((getDim() + subDim - 1) / subDim);

    if (null == rows[idx]) {
      LongIntVector[] tmpParts = new LongIntVector[numComp];
      for (int i = 0; i < numComp; i++) {
        LongIntSparseVectorStorage storage = new LongIntSparseVectorStorage(subDim);
        tmpParts[i] = new LongIntVector(matrixId, idx, clock, (long) getDim(), storage);
      }
      CompLongIntVector tmpVect =
          new CompLongIntVector(matrixId, idx, clock, (long) getDim(), tmpParts, subDim);
      rows[idx] = tmpVect;
    }
  }

  @Override
  public double min() {
    double minVal = Double.MAX_VALUE;
    for (CompLongIntVector ele : rows) {
      if (null != ele) {
        double rowMin = ele.min();
        if (rowMin < minVal) {
          minVal = rowMin;
        }
      }
    }

    if (minVal == Double.MAX_VALUE) {
      minVal = Double.NaN;
    }

    return minVal;
  }

  @Override
  public Vector min(int axis) {
    assert axis == 1;
    double[] minArr = new double[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i] != null) {
        minArr[i] = rows[i].min();
      } else {
        minArr[i] = Double.NaN;
      }

    }
    return VFactory.denseDoubleVector(matrixId, 0, clock, minArr);
  }

  @Override
  public Vector max(int axis) {
    assert axis == 1;
    double[] maxArr = new double[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i] != null) {
        maxArr[i] = rows[i].max();
      } else {
        maxArr[i] = Double.NaN;
      }

    }
    return VFactory.denseDoubleVector(matrixId, 0, clock, maxArr);
  }

  @Override
  public double max() {
    double maxVal = Double.MIN_VALUE;
    for (CompLongIntVector ele : rows) {
      if (null != ele) {
        double rowMin = ele.min();
        if (rowMin > maxVal) {
          maxVal = rowMin;
        }
      }
    }

    if (maxVal == Double.MIN_VALUE) {
      maxVal = Double.NaN;
    }

    return maxVal;
  }

  @Override
  public Vector sum(int axis) {
    assert axis == 1;
    double[] maxArr = new double[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i] != null) {
        maxArr[i] = rows[i].sum();
      } else {
        maxArr[i] = Double.NaN;
      }
    }
    return VFactory.denseDoubleVector(matrixId, 0, clock, maxArr);
  }

  @Override
  public Vector average(int axis) {
    assert axis == 1;
    double[] maxArr = new double[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i] != null) {
        maxArr[i] = rows[i].average();
      } else {
        maxArr[i] = Double.NaN;
      }
    }
    return VFactory.denseDoubleVector(matrixId, 0, clock, maxArr);
  }

  @Override
  public Vector std(int axis) {
    assert axis == 1;
    double[] maxArr = new double[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i] != null) {
        maxArr[i] = rows[i].std();
      } else {
        maxArr[i] = Double.NaN;
      }
    }
    return VFactory.denseDoubleVector(matrixId, 0, clock, maxArr);
  }

  @Override
  public Vector norm(int axis) {
    assert axis == 1;
    double[] maxArr = new double[rows.length];
    for (int i = 0; i < rows.length; i++) {
      if (rows[i] != null) {
        maxArr[i] = rows[i].norm();
      } else {
        maxArr[i] = Double.NaN;
      }
    }
    return VFactory.denseDoubleVector(matrixId, 0, clock, maxArr);
  }

  @Override
  public Matrix copy() {
    CompLongIntVector[] newRows = new CompLongIntVector[rows.length];
    for (int i = 0; i < rows.length; i++) {
      newRows[i] = rows[i].copy();
    }
    return new RBCompLongIntMatrix(matrixId, clock, newRows);
  }
}