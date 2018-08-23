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
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.vector.Vector;

public class UnaryMatrixExecutor {
  public static Matrix apply(Matrix mat, Unary op) {
    if (mat instanceof BlasDoubleMatrix) {
      return apply((BlasDoubleMatrix) mat, op);
    } else if (mat instanceof BlasFloatMatrix) {
      return apply((BlasFloatMatrix) mat, op);
    } else if (mat instanceof RowBasedMatrix) {
      return ((RowBasedMatrix) mat).calulate(op);
    } else {
      throw new AngelException("The operation is not supported!");
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat, Unary op) {
    double[] data = mat.getData();
    int size = mat.getNumRows() * mat.getNumCols();
    if (op.isInplace()) {
      for (int i = 0; i < size; i++) {
        data[i] = op.apply(data[i]);
      }
      return mat;
    } else {
      double[] newData = new double[size];
      for (int i = 0; i < size; i++) {
        newData[i] = op.apply(data[i]);
      }
      return new BlasDoubleMatrix(mat.getMatrixId(), mat.getClock(), mat.getNumRows(),
        mat.getNumCols(), newData);
    }
  }

  private static Matrix apply(BlasFloatMatrix mat, Unary op) {
    float[] data = mat.getData();
    int size = mat.getNumRows() * mat.getNumCols();
    if (op.isInplace()) {
      for (int i = 0; i < size; i++) {
        data[i] = op.apply(data[i]);
      }
      return mat;
    } else {
      float[] newData = new float[size];
      for (int i = 0; i < size; i++) {
        newData[i] = op.apply(data[i]);
      }
      return new BlasFloatMatrix(mat.getMatrixId(), mat.getClock(), mat.getNumRows(),
        mat.getNumCols(), newData);
    }
  }
}
