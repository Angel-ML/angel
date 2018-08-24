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


package com.tencent.angel.ml.math2.utils;


import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.*;
import com.tencent.angel.ml.math2.vector.*;

public class MatrixUtils {
  public static BlasDoubleMatrix rbCompDense2Blas(RBCompIntDoubleMatrix mat) {
    assert mat != null;

    int dim = (int) mat.getDim();
    int subDim = mat.getSubDim();
    CompIntDoubleVector[] rows = mat.getRows();

    double[] data = new double[rows.length * dim];
    int rowId = 0;
    for (CompIntDoubleVector row : rows) {
      IntDoubleVector[] partitions = row.getPartitions();
      int partId = 0;
      for (IntDoubleVector part : partitions) {
        assert part.isDense();
        double[] src = part.getStorage().getValues();
        System.arraycopy(src, 0, data, rowId * dim + partId * subDim, src.length);
        partId += 1;
      }
      rowId += 1;
    }

    return MFactory.denseDoubleMatrix(rows.length, dim, data);
  }

  public static BlasFloatMatrix rbCompDense2Blas(RBCompIntFloatMatrix mat) {
    assert mat != null;

    int dim = (int) mat.getDim();
    int subDim = mat.getSubDim();
    CompIntFloatVector[] rows = mat.getRows();

    float[] data = new float[rows.length * dim];
    int rowId = 0;
    for (CompIntFloatVector row : rows) {
      IntFloatVector[] partitions = row.getPartitions();
      int partId = 0;
      for (IntFloatVector part : partitions) {
        assert part.isDense();
        float[] src = part.getStorage().getValues();
        System.arraycopy(src, 0, data, rowId * dim + partId * subDim, src.length);
        partId += 1;
      }
      rowId += 1;
    }

    return MFactory.denseFloatMatrix(rows.length, dim, data);
  }

  public static RBCompIntDoubleMatrix blas2RBCompDense(BlasDoubleMatrix mat, int subDim) {
    int dim = mat.getNumCols();
    int numRows = mat.getNumRows();
    int numComp = (dim + subDim - 1) / subDim;

    double[] data = mat.getData();
    CompIntDoubleVector[] rows = new CompIntDoubleVector[numRows];
    for (int row = 0; row < numRows; row++) {
      IntDoubleVector[] parts = new IntDoubleVector[numComp];
      for (int i = 0; i < numComp; i++) {
        int thisSubDim;
        if ((i + 1) * subDim > dim) {
          thisSubDim = dim - i * subDim;
        } else {
          thisSubDim = subDim;
        }

        double[] part = new double[thisSubDim];
        System.arraycopy(data, row * dim + i * subDim, part, 0, thisSubDim);
        parts[i] = VFactory.denseDoubleVector(part);
      }
      rows[row] = VFactory.compIntDoubleVector(dim, parts);
    }

    return MFactory.rbCompIntDoubleMatrix(rows);
  }

  public static RBCompIntFloatMatrix blas2RBCompDense(BlasFloatMatrix mat, int subDim) {
    int dim = mat.getNumCols();
    int numRows = mat.getNumRows();
    int numComp = (dim + subDim - 1) / subDim;

    float[] data = mat.getData();
    CompIntFloatVector[] rows = new CompIntFloatVector[numRows];
    for (int row = 0; row < numRows; row++) {
      IntFloatVector[] parts = new IntFloatVector[numComp];
      for (int i = 0; i < numComp; i++) {
        int thisSubDim;
        if ((i + 1) * subDim > dim) {
          thisSubDim = dim - i * subDim;
        } else {
          thisSubDim = subDim;
        }

        float[] part = new float[thisSubDim];
        System.arraycopy(data, row * dim + i * subDim, part, 0, thisSubDim);
        parts[i] = VFactory.denseFloatVector(part);
      }
      rows[row] = VFactory.compIntFloatVector(dim, parts);
    }

    return MFactory.rbCompIntFloatMatrix(rows);
  }

  public static Matrix emptyLike(Matrix mat) {
    if (mat instanceof BlasMatrix) {
      return emptyLike((BlasMatrix) mat);
    } else if (mat instanceof RBCompIntDoubleMatrix) {
      return emptyLike((RBCompIntDoubleMatrix) mat);
    } else if (mat instanceof RBCompIntFloatMatrix) {
      return emptyLike((RBCompIntFloatMatrix) mat);
    } else {
      throw new AngelException("The matrix type is not supported!");
    }
  }

  private static BlasMatrix emptyLike(BlasMatrix mat) {
    int rows = mat.getNumRows();
    int cols = mat.getNumCols();
    if (mat instanceof BlasDoubleMatrix) {
      return MFactory.denseDoubleMatrix(rows, cols);
    } else {
      return MFactory.denseFloatMatrix(rows, cols);
    }

  }

  private static RBCompIntDoubleMatrix emptyLike(RBCompIntDoubleMatrix mat) {
    int numRows = mat.getNumRows();
    CompIntDoubleVector[] rows = new CompIntDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = (CompIntDoubleVector) VectorUtils.emptyLike(mat.getRow(i));
    }

    return MFactory.rbCompIntDoubleMatrix(rows);
  }

  private static RBCompIntFloatMatrix emptyLike(RBCompIntFloatMatrix mat) {
    int numRows = mat.getNumRows();
    CompIntFloatVector[] rows = new CompIntFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = (CompIntFloatVector) VectorUtils.emptyLike(mat.getRow(i));
    }

    return MFactory.rbCompIntFloatMatrix(rows);
  }
}