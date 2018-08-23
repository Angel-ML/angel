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


package com.tencent.angel.ml.math2.ufuncs.executor.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.matrix.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import static com.tencent.angel.ml.math2.ufuncs.expression.OpType.*;

public class BinaryMatrixExecutor {
  public static Matrix apply(Matrix mat, Vector v, int idx, boolean onCol, Binary op) {
    if (mat instanceof BlasDoubleMatrix && v instanceof IntDoubleVector) {
      return apply((BlasDoubleMatrix) mat, (IntDoubleVector) v, idx, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntFloatVector) {
      return apply((BlasDoubleMatrix) mat, (IntFloatVector) v, idx, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntLongVector) {
      return apply((BlasDoubleMatrix) mat, (IntLongVector) v, idx, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntIntVector) {
      return apply((BlasDoubleMatrix) mat, (IntIntVector) v, idx, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntDummyVector) {
      return apply((BlasDoubleMatrix) mat, (IntDummyVector) v, idx, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntFloatVector) {
      return apply((BlasFloatMatrix) mat, (IntFloatVector) v, idx, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntLongVector) {
      return apply((BlasFloatMatrix) mat, (IntLongVector) v, idx, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntIntVector) {
      return apply((BlasFloatMatrix) mat, (IntIntVector) v, idx, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntDummyVector) {
      return apply((BlasFloatMatrix) mat, (IntDummyVector) v, idx, onCol, op);
    } else if (mat instanceof RowBasedMatrix) {
      assert !onCol;
      return ((RowBasedMatrix) mat).calulate(idx, v, op);
    } else {
      throw new AngelException("The operation is not supported!");
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntDummyVector v, int idx, boolean onCol,
    Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = new byte[v.getDim()];
    if (onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        data[i * n + idx] = op.apply(data[i * n + idx], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            data[i * n + idx] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            data[i * n + idx] = op.apply(data[i * n + idx], 0);
          }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData = ArrayCopy.copy(data);
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        newData[i * n + idx] = op.apply(data[i * n + idx], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            newData[i * n + idx] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            newData[i * n + idx] = op.apply(newData[i * n + idx], 0);
          }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int j = idxs[k];
        flag[j] = 1;
        data[idx * n + j] = op.apply(data[idx * n + j], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            data[idx * n + j] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            data[idx * n + j] = op.apply(data[idx * n + j], 0);
          }
        }
      }
      return mat;
    } else {
      double[] newData = ArrayCopy.copy(data);
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int j = idxs[k];
        flag[j] = 1;
        newData[idx * n + j] = op.apply(data[idx * n + j], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            newData[idx * n + j] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            newData[idx * n + j] = op.apply(newData[idx * n + j], 0);
          }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntDoubleVector v, int idx, boolean onCol,
    Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getDoubleValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getDoubleValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getDoubleValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getDoubleValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntFloatVector v, int idx, boolean onCol,
    Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntLongVector v, int idx, boolean onCol,
    Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntIntVector v, int idx, boolean onCol,
    Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntDummyVector v, int idx, boolean onCol,
    Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = new byte[v.getDim()];
    if (onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        data[i * n + idx] = op.apply(data[i * n + idx], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            data[i * n + idx] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            data[i * n + idx] = op.apply(data[i * n + idx], 0);
          }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData = ArrayCopy.copy(data);
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        newData[i * n + idx] = op.apply(data[i * n + idx], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            newData[i * n + idx] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            newData[i * n + idx] = op.apply(newData[i * n + idx], 0);
          }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int j = idxs[k];
        flag[j] = 1;
        data[idx * n + j] = op.apply(data[idx * n + j], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            data[idx * n + j] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            data[idx * n + j] = op.apply(data[idx * n + j], 0);
          }
        }
      }
      return mat;
    } else {
      float[] newData = ArrayCopy.copy(data);
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int j = idxs[k];
        flag[j] = 1;
        newData[idx * n + j] = op.apply(data[idx * n + j], 1);
      }

      if (op.getOpType() == INTERSECTION) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            newData[idx * n + j] = 0;
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            newData[idx * n + j] = op.apply(newData[idx * n + j], 0);
          }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntFloatVector v, int idx, boolean onCol,
    Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getFloatValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntLongVector v, int idx, boolean onCol,
    Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getLongValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntIntVector v, int idx, boolean onCol,
    Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          data[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          data[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                data[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          newData[i * n + idx] = op.apply(data[i * n + idx], values[i]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          newData[i * n + idx] = op.apply(data[i * n + idx], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                newData[i * n + idx] = op.apply(data[i * n + idx], 0);
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          data[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = 0;
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                data[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          newData[idx * n + j] = op.apply(data[idx * n + j], values[j]);
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], entry.getIntValue());
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[idx * n + j] = op.apply(data[idx * n + j], values[k]);
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                newData[idx * n + j] = op.apply(data[idx * n + j], 0);
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }


  public static Matrix apply(Matrix mat, Vector v, boolean onCol, Binary op) {
    if (mat instanceof BlasDoubleMatrix && v instanceof IntDoubleVector) {
      return apply((BlasDoubleMatrix) mat, (IntDoubleVector) v, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntFloatVector) {
      return apply((BlasDoubleMatrix) mat, (IntFloatVector) v, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntLongVector) {
      return apply((BlasDoubleMatrix) mat, (IntLongVector) v, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntIntVector) {
      return apply((BlasDoubleMatrix) mat, (IntIntVector) v, onCol, op);
    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntDummyVector) {
      return apply((BlasDoubleMatrix) mat, (IntDummyVector) v, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntFloatVector) {
      return apply((BlasFloatMatrix) mat, (IntFloatVector) v, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntLongVector) {
      return apply((BlasFloatMatrix) mat, (IntLongVector) v, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntIntVector) {
      return apply((BlasFloatMatrix) mat, (IntIntVector) v, onCol, op);
    } else if (mat instanceof BlasFloatMatrix && v instanceof IntDummyVector) {
      return apply((BlasFloatMatrix) mat, (IntDummyVector) v, onCol, op);
    } else if (mat instanceof RowBasedMatrix) {
      assert !onCol;
      return ((RowBasedMatrix) mat).calulate(v, op);
    } else {
      throw new AngelException("The operation is not supported!");
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntDummyVector v, boolean onCol, Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = new byte[v.getDim()];


    if (onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        for (int j = 0; j < n; j++) {
          data[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == INTERSECTION) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            for (int j = 0; j < n; j++) {
              data[i * n + j] = 0;
            }
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            for (int j = 0; j < n; j++) {
              data[i * n + j] = op.apply(data[i * n + j], 0);
            }
          }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        for (int j = 0; j < n; j++) {
          newData[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            for (int j = 0; j < n; j++) {
              newData[i * n + j] = op.apply(newData[i * n + j], 0);
            }
          }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int i = 0; i < n; i++) {
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == INTERSECTION) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            for (int i = 0; i < m; i++) {
              data[i * n + j] = 0;
            }
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            for (int i = 0; i < m; i++) {
              data[i * n + j] = op.apply(data[i * n + j], 0);
            }
          }
        }
      }

      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      int[] idxs = v.getIndices();
      for (int i = 0; i < n; i++) {
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            for (int i = 0; i < m; i++) {
              newData[i * n + j] = op.apply(newData[i * n + j], 0);
            }
          }
        }
      }
      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntDoubleVector v, boolean onCol, Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          double value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getDoubleValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          double value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          double value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getDoubleValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          double value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          double value = entry.getDoubleValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          double value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        double[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          double value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          double value = entry.getDoubleValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        double[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          double value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntFloatVector v, boolean onCol, Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          float value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getFloatValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          float value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          float value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getFloatValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          float value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          double value = entry.getFloatValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          float value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          float value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          double value = entry.getFloatValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          float value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntLongVector v, boolean onCol, Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          long value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getLongValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          long value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          long value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getLongValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          long value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          double value = entry.getLongValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          long value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          long value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          double value = entry.getLongValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          long value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, IntIntVector v, boolean onCol, Binary op) {
    double[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          int value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getIntValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          int value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          int value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          double value = entry.getIntValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          int value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          double value = entry.getIntValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          int value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      double[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new double[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          int value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          double value = entry.getIntValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          int value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntDummyVector v, boolean onCol, Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = new byte[v.getDim()];


    if (onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        for (int j = 0; j < n; j++) {
          data[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == INTERSECTION) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            for (int j = 0; j < n; j++) {
              data[i * n + j] = 0;
            }
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            for (int j = 0; j < n; j++) {
              data[i * n + j] = op.apply(data[i * n + j], 0);
            }
          }
        }
      }
      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      int[] idxs = v.getIndices();
      for (int k = 0; k < size; k++) {
        int i = idxs[k];
        flag[i] = 1;
        for (int j = 0; j < n; j++) {
          newData[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == ALL) {
        for (int i = 0; i < m; i++) {
          if (flag[i] == 0) {
            for (int j = 0; j < n; j++) {
              newData[i * n + j] = op.apply(newData[i * n + j], 0);
            }
          }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      int[] idxs = v.getIndices();
      for (int i = 0; i < n; i++) {
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          data[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == INTERSECTION) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            for (int i = 0; i < m; i++) {
              data[i * n + j] = 0;
            }
          }
        }
      } else if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            for (int i = 0; i < m; i++) {
              data[i * n + j] = op.apply(data[i * n + j], 0);
            }
          }
        }
      }

      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      int[] idxs = v.getIndices();
      for (int i = 0; i < n; i++) {
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          newData[i * n + j] = op.apply(data[i * n + j], 1);
        }
      }

      if (op.getOpType() == ALL) {
        for (int j = 0; j < n; j++) {
          if (flag[j] == 0) {
            for (int i = 0; i < m; i++) {
              newData[i * n + j] = op.apply(newData[i * n + j], 0);
            }
          }
        }
      }
      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntFloatVector v, boolean onCol, Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          float value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          float value = entry.getFloatValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          float value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          float value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          float value = entry.getFloatValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          float value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          float value = entry.getFloatValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          float value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        float[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          float value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          float value = entry.getFloatValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        float[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          float value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntLongVector v, boolean onCol, Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          long value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          float value = entry.getLongValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          long value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          long value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          float value = entry.getLongValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          long value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          float value = entry.getLongValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          long value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        long[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          long value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          float value = entry.getLongValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        long[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          long value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, IntIntVector v, boolean onCol, Binary op) {
    float[] data = mat.getData();
    int m = mat.getNumRows(), n = mat.getNumCols();

    int size = v.size();
    byte[] flag = null;
    if (!v.isDense()) {
      flag = new byte[v.getDim()];
    }

    if (onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          int value = values[i];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          float value = entry.getIntValue();
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          int value = values[k];
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return mat;
    } else if (onCol && !op.isInplace()) {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          int value = values[i];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int i = entry.getIntKey();
          flag[i] = 1;
          float value = entry.getIntValue();
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int i = idxs[k];
          flag[i] = 1;
          int value = values[k];
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }
      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int i = 0; i < m; i++) {
              if (flag[i] == 0) {
                for (int j = 0; j < n; j++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }

      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    } else if (!onCol && op.isInplace()) {
      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            data[i * n + j] = op.apply(data[i * n + j], values[j]);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          float value = entry.getIntValue();
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          int value = values[k];
          flag[j] = 1;
          for (int i = 0; i < m; i++) {
            data[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = 0;
                }
              }
            }
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  data[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return mat;
    } else {
      float[] newData;
      if (op.getOpType() == INTERSECTION) {
        newData = new float[m * n];
      } else {
        newData = ArrayCopy.copy(data);
      }

      if (v.isDense()) {
        int[] values = v.getStorage().getValues();
        for (int j = 0; j < n; j++) {
          int value = values[j];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else if (v.isSparse()) {
        ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int j = entry.getIntKey();
          flag[j] = 1;
          float value = entry.getIntValue();
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      } else { // sorted
        int[] idxs = v.getStorage().getIndices();
        int[] values = v.getStorage().getValues();
        for (int k = 0; k < size; k++) {
          int j = idxs[k];
          flag[j] = 1;
          int value = values[k];
          for (int i = 0; i < m; i++) {
            newData[i * n + j] = op.apply(data[i * n + j], value);
          }
        }
      }

      if (!v.isDense()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            break;
          case UNION:
            break;
          case ALL:
            for (int j = 0; j < n; j++) {
              if (flag[j] == 0) {
                for (int i = 0; i < m; i++) {
                  newData[i * n + j] = op.apply(data[i * n + j], 0);
                }
              }
            }
        }
      }
      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), m, n, newData);
    }
  }



  public static Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2, Binary op) {
    if (mat1 instanceof BlasMatrix && mat2 instanceof BlasMatrix) {
      if (trans1 == trans2) {
        assert ((BlasMatrix) mat1).getNumRows() == ((BlasMatrix) mat2).getNumRows();
        assert ((BlasMatrix) mat1).getNumCols() == ((BlasMatrix) mat2).getNumCols();
      } else {
        assert ((BlasMatrix) mat1).getNumRows() == ((BlasMatrix) mat2).getNumCols();
        assert ((BlasMatrix) mat1).getNumCols() == ((BlasMatrix) mat2).getNumRows();
      }
    } else if (mat1 instanceof RowBasedMatrix && mat2 instanceof RowBasedMatrix) {
      assert !trans1 && !trans2;
      int numRow1 = ((RowBasedMatrix) mat1).getRows().length;
      int numRow2 = ((RowBasedMatrix) mat2).getRows().length;
      assert numRow1 == numRow2;
      assert ((RowBasedMatrix) mat1).getDim() == ((RowBasedMatrix) mat2).getDim();
    } else {
      throw new AngelException("The operation is not supported!");
    }

    if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof BlasDoubleMatrix) {
      return apply((BlasDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2, op);
    } else if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof BlasFloatMatrix) {
      return apply((BlasDoubleMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2, op);
    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof BlasFloatMatrix) {
      return apply((BlasFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2, op);
    } else if (mat1 instanceof RowBasedMatrix) {
      return ((RowBasedMatrix) mat1).calulate(mat2, op);
    } else {
      throw new AngelException("The operation is not supported!");
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat1, boolean trans1, BlasDoubleMatrix mat2,
    boolean trans2, Binary op) {
    double[] mat1Data = mat1.getData();
    double[] mat2Data = mat2.getData();
    int m = mat1.getNumRows(), n = mat1.getNumCols();
    int p = mat2.getNumRows(), q = mat2.getNumCols();
    int size = m * n;

    if (trans1 && trans2) { // TT
      double[] newData = new double[size];
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[j * q + i]);
        }
      }
      return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, m, newData);
    } else if (!trans1 && trans2) { // _T
      if (op.isInplace()) {
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            mat1Data[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);
          }
        }
        return mat1;
      } else {
        double[] newData = new double[size];
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);
          }
        }
        return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
      }
    } else if (trans1 && !trans2) { //T_
      double[] newData = new double[size];
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[i * q + j]);
        }
      }
      return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
    } else {
      if (op.isInplace()) {
        for (int i = 0; i < size; i++) {
          mat1Data[i] = op.apply(mat1Data[i], mat2Data[i]);
        }
        return mat1;
      } else {
        double[] newData = new double[size];
        for (int i = 0; i < size; i++) {
          newData[i] = op.apply(mat1Data[i], mat2Data[i]);
        }
        return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
      }
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat1, boolean trans1, BlasFloatMatrix mat2,
    boolean trans2, Binary op) {
    double[] mat1Data = mat1.getData();
    float[] mat2Data = mat2.getData();
    int m = mat1.getNumRows(), n = mat1.getNumCols();
    int p = mat2.getNumRows(), q = mat2.getNumCols();
    int size = m * n;

    if (trans1 && trans2) { // TT
      double[] newData = new double[size];
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[j * q + i]);
        }
      }
      return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, m, newData);
    } else if (!trans1 && trans2) { // _T
      if (op.isInplace()) {
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            mat1Data[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);
          }
        }
        return mat1;
      } else {
        double[] newData = new double[size];
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);
          }
        }
        return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
      }
    } else if (trans1 && !trans2) { //T_
      double[] newData = new double[size];
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[i * q + j]);
        }
      }
      return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
    } else {
      if (op.isInplace()) {
        for (int i = 0; i < size; i++) {
          mat1Data[i] = op.apply(mat1Data[i], mat2Data[i]);
        }
        return mat1;
      } else {
        double[] newData = new double[size];
        for (int i = 0; i < size; i++) {
          newData[i] = op.apply(mat1Data[i], mat2Data[i]);
        }
        return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
      }
    }
  }

  private static Matrix apply(BlasFloatMatrix mat1, boolean trans1, BlasFloatMatrix mat2,
    boolean trans2, Binary op) {
    float[] mat1Data = mat1.getData();
    float[] mat2Data = mat2.getData();
    int m = mat1.getNumRows(), n = mat1.getNumCols();
    int p = mat2.getNumRows(), q = mat2.getNumCols();
    int size = m * n;

    if (trans1 && trans2) { // TT
      float[] newData = new float[size];
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[j * q + i]);
        }
      }
      return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), n, m, newData);
    } else if (!trans1 && trans2) { // _T
      if (op.isInplace()) {
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            mat1Data[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);
          }
        }
        return mat1;
      } else {
        float[] newData = new float[size];
        for (int i = 0; i < m; i++) {
          for (int j = 0; j < n; j++) {
            newData[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);
          }
        }
        return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
      }
    } else if (trans1 && !trans2) { //T_
      float[] newData = new float[size];
      for (int i = 0; i < n; i++) {
        for (int j = 0; j < m; j++) {
          newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[i * q + j]);
        }
      }
      return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
    } else {
      if (op.isInplace()) {
        for (int i = 0; i < size; i++) {
          mat1Data[i] = op.apply(mat1Data[i], mat2Data[i]);
        }
        return mat1;
      } else {
        float[] newData = new float[size];
        for (int i = 0; i < size; i++) {
          newData[i] = op.apply(mat1Data[i], mat2Data[i]);
        }
        return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);
      }
    }
  }
}
