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

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.tencent.angel.ml.math2.VFactory;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class BlasFloatMatrix extends BlasMatrix {
  private float[] data;

  public BlasFloatMatrix() {
    super();
  }

  public BlasFloatMatrix(int matrixId, int clock, int numRows, int numCols, float[] data) {
    this.matrixId = matrixId;
    this.clock = clock;
    this.numRows = numRows;
    this.numCols = numCols;
    this.data = data;
  }

  public BlasFloatMatrix(int numRows, int numCols, float[] data) {
    this(0, 0, numRows, numCols, data);
  }

  public float[] getData() {
    return data;
  }

  public void setData(float[] data) {
    this.data = data;
  }

  @Override public double min() {
    double minVal = Float.MAX_VALUE;
    for (int k = 0; k < numRows * numCols; k++) {
      if (data[k] < minVal) {
        minVal = data[k];
      }
    }

    return minVal;
  }

  @Override public Vector min(int axis) {
    // axis = 0: on rows
    // axis = 1: on cols
    assert (axis == 0 || axis == 1);
    float[] rdVec = null;
    switch (axis) {
      case 0:  // on row
        rdVec = new float[numCols];
        for (int j = 0; j < numCols; j++) {
          rdVec[j] = Float.MAX_VALUE;
        }

        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            if (data[i * numCols + j] < rdVec[j]) {
              rdVec[j] = data[i * numCols + j];
            }
          }
        }
        break;
      case 1:
        rdVec = new float[numRows];
        for (int i = 0; i < numRows; i++) {
          rdVec[i] = Float.MAX_VALUE;
        }

        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            if (data[i * numCols + j] < rdVec[i]) {
              rdVec[i] = data[i * numCols + j];
            }
          }
        }
        break;
    }

    return VFactory.denseFloatVector(matrixId, 0, clock, rdVec);
  }

  @Override public double max() {
    double maxVal = Float.MIN_VALUE;
    for (int k = 0; k < numRows * numCols; k++) {
      if (data[k] > maxVal) {
        maxVal = data[k];
      }
    }

    return maxVal;
  }

  @Override public Vector max(int axis) {
    // axis = 0: on rows
    // axis = 1: on cols
    assert (axis == 0 || axis == 1);
    float[] rdVec = null;
    switch (axis) {
      case 0:  // on row
        rdVec = new float[numCols];
        for (int j = 0; j < numCols; j++) {
          rdVec[j] = Float.MIN_VALUE;
        }

        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            if (data[i * numCols + j] > rdVec[j]) {
              rdVec[j] = data[i * numCols + j];
            }
          }
        }
        break;
      case 1:
        rdVec = new float[numRows];
        for (int i = 0; i < numRows; i++) {
          rdVec[i] = Float.MIN_VALUE;
        }

        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            if (data[i * numCols + j] > rdVec[i]) {
              rdVec[i] = data[i * numCols + j];
            }
          }
        }
        break;
    }

    return VFactory.denseFloatVector(matrixId, 0, clock, rdVec);
  }

  public Vector argmax(int axis) {
    // axis = 0: on rows
    // axis = 1: on cols
    assert (axis == 0 || axis == 1);
    float[] rdVec = null;
    float[] idxVec = null;
    switch (axis) {
      case 0:  // on row
        rdVec = new float[numCols];
        idxVec = new float[numCols];
        for (int j = 0; j < numCols; j++) {
          rdVec[j] = Float.MIN_VALUE;
          idxVec[j] = -1;
        }

        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            if (data[i * numCols + j] > rdVec[j]) {
              rdVec[j] = data[i * numCols + j];
              idxVec[j] = i;
            }
          }
        }
        break;
      case 1:
        rdVec = new float[numRows];
        idxVec = new float[numRows];
        for (int i = 0; i < numRows; i++) {
          rdVec[i] = Float.MIN_VALUE;
          idxVec[i] = -1;
        }

        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            if (data[i * numCols + j] > rdVec[i]) {
              rdVec[i] = data[i * numCols + j];
              idxVec[i] = j;
            }
          }
        }
        break;
    }

    return VFactory.denseFloatVector(matrixId, 0, clock, idxVec);
  }

  @Override public double sum() {
    double res = 0.0;
    for (float value : data) {
      res += value;
    }
    return res;
  }

  @Override public Vector sum(int axis) {
    // axis = 0: on rows
    // axis = 1: on cols
    assert (axis == 0 || axis == 1);
    float[] rdVec = null;
    switch (axis) {
      case 0:  // on row
        rdVec = new float[numCols];
        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            rdVec[j] += data[i * numCols + j];
          }
        }
        break;
      case 1:
        rdVec = new float[numRows];
        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            rdVec[i] += data[i * numCols + j];
          }
        }
        break;
    }

    return VFactory.denseFloatVector(matrixId, 0, clock, rdVec);
  }

  @Override public double std() {
    double sum1 = 0.0, sum2 = 0.0;
    for (float value : data) {
      sum1 += value;
      sum2 += value * sum1;
    }

    sum1 /= numRows * numCols;
    sum2 /= numRows * numCols;
    return Math.sqrt(sum2 - sum1 * sum1);
  }

  @Override public Vector std(int axis) {
    // axis = 0: on rows
    // axis = 1: on cols
    assert (axis == 0 || axis == 1);
    float[] rdVec = null;
    float[] rdVec2 = null;
    switch (axis) {
      case 0:  // on row
        rdVec = new float[numCols];
        rdVec2 = new float[numCols];
        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            float value = data[i * numCols + j];
            rdVec[j] += value;
            rdVec2[j] += value * value;
          }
        }

        for (int j = 0; j < numCols; j++) {
          float avg1 = rdVec[j] / numRows;
          float avg2 = rdVec2[j] / numRows;
          rdVec[j] = (float) Math.sqrt(avg2 - avg1 * avg1);
        }
        break;
      case 1:
        rdVec = new float[numRows];
        rdVec2 = new float[numRows];
        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            float value = data[i * numCols + j];
            rdVec[j] += value;
            rdVec2[j] += value * value;
          }
        }

        for (int i = 0; i < numRows; i++) {
          float avg1 = rdVec[i] / numCols;
          float avg2 = rdVec2[i] / numCols;
          rdVec[i] = (float) Math.sqrt(avg2 - avg1 * avg1);
        }
        break;
    }

    return VFactory.denseFloatVector(matrixId, 0, clock, rdVec);
  }

  @Override public double average() {
    return sum() / (numRows * numCols);
  }

  @Override public Vector average(int axis) {
    assert (axis == 0 || axis == 1);
    Vector res = null;
    switch (axis) {
      case 0:
        res = sum(axis).idiv(numRows);
        break;
      case 1:
        res = sum(axis).idiv(numCols);
        break;
    }

    return res;
  }

  @Override public double norm() {
    double res = 0.0;
    for (float value : data) {
      res += value * value;
    }
    return Math.sqrt(res);
  }

  @Override public Vector norm(int axis) {
    // axis = 0: on rows
    // axis = 1: on cols
    assert (axis == 0 || axis == 1);
    float[] rdVec = null;
    switch (axis) {
      case 0:  // on row
        rdVec = new float[numCols];
        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            float value = data[i * numCols + j];
            rdVec[j] += value * value;
          }
        }

        for (int j = 0; j < numCols; j++) {
          rdVec[j] = (float) Math.sqrt(rdVec[j]);
        }
        break;
      case 1:
        rdVec = new float[numRows];
        for (int i = 0; i < numRows; i++) {
          for (int j = 0; j < numCols; j++) {
            float value = data[i * numCols + j];
            rdVec[i] += value * value;
          }
        }

        for (int i = 0; i < numRows; i++) {
          rdVec[i] = (float) Math.sqrt(rdVec[i]);
        }
        break;
    }

    return VFactory.denseFloatVector(matrixId, 0, clock, rdVec);
  }

  @Override public Vector diag() {
    int numDiag = Math.min(numRows, numCols);
    float[] resArr = new float[numDiag];
    for (int i = 0; i < numDiag; i++) {
      resArr[i] = data[i * numRows + i];
    }

    IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(resArr);
    return new IntFloatVector(getMatrixId(), 0, getClock(), resArr.length, storage);
  }

  @Override public void clear() {
    for (int i = 0; i < numCols * numRows; i++) {
      data[i] = 0;
    }

    matrixId = 0;
    clock = 0;
    numRows = 0;
    numCols = 0;
  }

  @Override public Matrix copy() {
    float[] newData = new float[numCols * numRows];
    System.arraycopy(data, 0, newData, 0, numCols * numRows);
    return new BlasFloatMatrix(matrixId, clock, numRows, numCols, newData);
  }

  public Matrix clone() {
    float[] newData = new float[numCols * numRows];
    System.arraycopy(data, 0, newData, 0, numCols * numRows);
    return new BlasFloatMatrix(matrixId, clock, numRows, numCols, newData);
  }

  public float get(int i, int j) {
    return data[i * numCols + j];
  }

  public void set(int i, int j, float value) {
    data[i * numCols + j] = value;
  }

  @Override public Vector getRow(int i) {
    float[] row = new float[numCols];
    System.arraycopy(data, i * numCols, row, 0, numCols);

    IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(row);
    return new IntFloatVector(getMatrixId(), i, getClock(), numCols, storage);
  }

  @Override public Vector getCol(int j) {
    float[] col = new float[numRows];

    for (int i = 0; i < numRows; i++) {
      col[i] = data[i * numCols + j];
    }

    IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(col);
    return new IntFloatVector(getMatrixId(), getClock(), 0, numRows, storage);
  }

  public Matrix setRow(int i, Vector v) {
    if (v instanceof IntFloatVector) {
      float[] rowData;
      if (v.isDense()) {
        rowData = ((IntFloatVector) v).getStorage().getValues();
      } else if (v.isSparse()) {
        rowData = new float[numCols];
        ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVector) v).getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          rowData[j] = entry.getFloatValue();
        }
      } else { // sorted
        rowData = new float[numCols];
        int[] idxs = ((IntFloatVector) v).getStorage().getIndices();
        float[] values = ((IntFloatVector) v).getStorage().getValues();
        int size = ((IntFloatVector) v).size();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          rowData[j] = values[k];
        }
      }

      System.arraycopy(rowData, 0, data, i * numCols, numCols);
    } else if (v instanceof IntLongVector) {
      long[] rowData;
      if (v.isDense()) {
        rowData = ((IntLongVector) v).getStorage().getValues();
      } else if (v.isSparse()) {
        rowData = new long[numCols];
        ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVector) v).getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          rowData[j] = entry.getLongValue();
        }
      } else { // sorted
        rowData = new long[numCols];
        int[] idxs = ((IntLongVector) v).getStorage().getIndices();
        long[] values = ((IntLongVector) v).getStorage().getValues();
        int size = ((IntLongVector) v).size();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          rowData[j] = values[k];
        }
      }

      for (int j = 0; j < numCols; j++) {
        data[i * numCols + j] = rowData[j];
      }
    } else if (v instanceof IntIntVector) {
      int[] rowData;
      if (v.isDense()) {
        rowData = ((IntIntVector) v).getStorage().getValues();
      } else if (v.isSparse()) {
        rowData = new int[numCols];
        ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVector) v).getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          rowData[j] = entry.getIntValue();
        }
      } else { // sorted
        rowData = new int[numCols];
        int[] idxs = ((IntIntVector) v).getStorage().getIndices();
        int[] values = ((IntIntVector) v).getStorage().getValues();
        int size = ((IntIntVector) v).size();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          rowData[j] = values[k];
        }
      }

      for (int j = 0; j < numCols; j++) {
        data[i * numCols + j] = rowData[j];
      }
    } else if (v instanceof IntDummyVector) {
      int[] rowData = new int[numCols];
      int[] idxs = ((IntDummyVector) v).getIndices();
      int size = ((IntDummyVector) v).size();
      for (int k = 0; k < size; k++) {
        int j = idxs[k];
        rowData[j] = 1;
      }

      for (int j = 0; j < numCols; j++) {
        data[i * numCols + j] = rowData[j];
      }
    } else {
      throw new AngelException("The operation is not supported!");
    }

    return this;
  }

  public Matrix setCol(int i, Vector v) {
    if (v instanceof IntFloatVector) {
      float[] rowData;
      if (v.isDense()) {
        rowData = ((IntFloatVector) v).getStorage().getValues();
      } else if (v.isSparse()) {
        rowData = new float[numRows];
        ObjectIterator<Int2FloatMap.Entry> iter = ((IntFloatVector) v).getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          rowData[j] = entry.getFloatValue();
        }
      } else { // sorted
        rowData = new float[numRows];
        int[] idxs = ((IntFloatVector) v).getStorage().getIndices();
        float[] values = ((IntFloatVector) v).getStorage().getValues();
        int size = ((IntFloatVector) v).size();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          rowData[j] = values[k];
        }
      }

      for (int j = 0; j < numRows; j++) {
        data[j * numCols + i] = rowData[j];
      }
    } else if (v instanceof IntLongVector) {
      long[] rowData;
      if (v.isDense()) {
        rowData = ((IntLongVector) v).getStorage().getValues();
      } else if (v.isSparse()) {
        rowData = new long[numRows];
        ObjectIterator<Int2LongMap.Entry> iter = ((IntLongVector) v).getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          rowData[j] = entry.getLongValue();
        }
      } else { // sorted
        rowData = new long[numRows];
        int[] idxs = ((IntLongVector) v).getStorage().getIndices();
        long[] values = ((IntLongVector) v).getStorage().getValues();
        int size = ((IntLongVector) v).size();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          rowData[j] = values[k];
        }
      }

      for (int j = 0; j < numRows; j++) {
        data[j * numCols + i] = rowData[j];
      }
    } else if (v instanceof IntIntVector) {
      int[] rowData;
      if (v.isDense()) {
        rowData = ((IntIntVector) v).getStorage().getValues();
      } else if (v.isSparse()) {
        rowData = new int[numRows];
        ObjectIterator<Int2IntMap.Entry> iter = ((IntIntVector) v).getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          rowData[j] = entry.getIntValue();
        }
      } else { // sorted
        rowData = new int[numRows];
        int[] idxs = ((IntIntVector) v).getStorage().getIndices();
        int[] values = ((IntIntVector) v).getStorage().getValues();
        int size = ((IntIntVector) v).size();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          rowData[j] = values[k];
        }
      }

      for (int j = 0; j < numRows; j++) {
        data[j * numCols + i] = rowData[j];
      }
    } else if (v instanceof IntDummyVector) {
      int[] rowData = new int[numRows];
      int[] idxs = ((IntDummyVector) v).getIndices();
      int size = ((IntDummyVector) v).size();
      for (int k = 0; k < size; k++) {
        int j = idxs[k];
        rowData[j] = 1;
      }

      for (int j = 0; j < numRows; j++) {
        data[j * numCols + i] = rowData[j];
      }
    } else {
      throw new AngelException("The operation is not supported!");
    }

    return this;
  }
}