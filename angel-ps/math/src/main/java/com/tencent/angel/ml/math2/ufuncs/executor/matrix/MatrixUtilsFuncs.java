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

import com.tencent.angel.ml.math2.matrix.*;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.*;


public interface MatrixUtilsFuncs {

  void onColDense(BlasDoubleMatrix mat, IntDoubleVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColDense(BlasDoubleMatrix mat, IntFloatVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColDense(BlasDoubleMatrix mat, IntLongVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColDense(BlasDoubleMatrix mat, IntIntVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColDense(BlasFloatMatrix mat, IntFloatVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColDense(BlasFloatMatrix mat, IntLongVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColDense(BlasFloatMatrix mat, IntIntVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColSparse(BlasDoubleMatrix mat, IntDoubleVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSparse(BlasDoubleMatrix mat, IntFloatVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSparse(BlasDoubleMatrix mat, IntLongVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSparse(BlasDoubleMatrix mat, IntIntVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSparse(BlasFloatMatrix mat, IntFloatVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColSparse(BlasFloatMatrix mat, IntLongVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColSparse(BlasFloatMatrix mat, IntIntVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColSorted(BlasDoubleMatrix mat, IntDoubleVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSorted(BlasDoubleMatrix mat, IntFloatVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSorted(BlasDoubleMatrix mat, IntLongVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSorted(BlasDoubleMatrix mat, IntIntVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColSorted(BlasFloatMatrix mat, IntFloatVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColSorted(BlasFloatMatrix mat, IntLongVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColSorted(BlasFloatMatrix mat, IntIntVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowDense(BlasDoubleMatrix mat, IntDoubleVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowDense(BlasDoubleMatrix mat, IntFloatVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowDense(BlasDoubleMatrix mat, IntLongVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowDense(BlasDoubleMatrix mat, IntIntVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowDense(BlasFloatMatrix mat, IntFloatVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowDense(BlasFloatMatrix mat, IntLongVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowDense(BlasFloatMatrix mat, IntIntVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasDoubleMatrix mat, IntDoubleVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasDoubleMatrix mat, IntFloatVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasDoubleMatrix mat, IntLongVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasDoubleMatrix mat, IntIntVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasFloatMatrix mat, IntFloatVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasFloatMatrix mat, IntLongVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowSparse(BlasFloatMatrix mat, IntIntVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasDoubleMatrix mat, IntDoubleVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasDoubleMatrix mat, IntFloatVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasDoubleMatrix mat, IntLongVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasDoubleMatrix mat, IntIntVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasFloatMatrix mat, IntFloatVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasFloatMatrix mat, IntLongVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowSorted(BlasFloatMatrix mat, IntIntVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onColDummy(BlasDoubleMatrix mat, IntDummyVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onColDummy(BlasFloatMatrix mat, IntDummyVector v, BlasFloatMatrix resMat, int idx, Binary op);

  void onRowDummy(BlasDoubleMatrix mat, IntDummyVector v, BlasDoubleMatrix resMat, int idx, Binary op);

  void onRowDummy(BlasFloatMatrix mat, IntDummyVector v, BlasFloatMatrix resMat, int idx, Binary op);


  Matrix transVsTrans(BlasDoubleMatrix mat1, BlasDoubleMatrix mat2, Binary op);

  Matrix transVsTrans(BlasDoubleMatrix mat1, BlasFloatMatrix mat2, Binary op);

  Matrix transVsTrans(BlasFloatMatrix mat1, BlasFloatMatrix mat2, Binary op);

  Matrix transVsNotTrans(BlasDoubleMatrix mat1, BlasDoubleMatrix mat2, Binary op);

  Matrix transVsNotTrans(BlasDoubleMatrix mat1, BlasFloatMatrix mat2, Binary op);

  Matrix transVsNotTrans(BlasFloatMatrix mat1, BlasFloatMatrix mat2, Binary op);

  Matrix notTransVsTrans(BlasDoubleMatrix mat1, BlasDoubleMatrix mat2, BlasDoubleMatrix resMat, Binary op);

  Matrix notTransVsTrans(BlasDoubleMatrix mat1, BlasFloatMatrix mat2, BlasDoubleMatrix resMat, Binary op);

  Matrix notTransVsTrans(BlasFloatMatrix mat1, BlasFloatMatrix mat2, BlasFloatMatrix resMat, Binary op);

  Matrix notTransVsNotTrans(BlasDoubleMatrix mat1, BlasDoubleMatrix mat2, BlasDoubleMatrix resMat, Binary op);

  Matrix notTransVsNotTrans(BlasDoubleMatrix mat1, BlasFloatMatrix mat2, BlasDoubleMatrix resMat, Binary op);

  Matrix notTransVsNotTrans(BlasFloatMatrix mat1, BlasFloatMatrix mat2, BlasFloatMatrix resMat, Binary op);


  void replace(BlasDoubleMatrix mat, int idx, Boolean onCol, byte[] flag, Binary op);

  void replace(BlasFloatMatrix mat, int idx, Boolean onCol, byte[] flag, Binary op);

  void matrixTransSparse(BlasDoubleMatrix mat, IntDoubleVector v, double[] resArr);

  void matrixTransSparse(BlasDoubleMatrix mat, IntFloatVector v, double[] resArr);

  void matrixTransSparse(BlasDoubleMatrix mat, IntLongVector v, double[] resArr);

  void matrixTransSparse(BlasDoubleMatrix mat, IntIntVector v, double[] resArr);

  void matrixTransSparse(BlasFloatMatrix mat, IntFloatVector v, float[] resArr);

  void matrixTransSparse(BlasFloatMatrix mat, IntLongVector v, float[] resArr);

  void matrixTransSparse(BlasFloatMatrix mat, IntIntVector v, float[] resArr);

  void matrixNotTransSparse(BlasDoubleMatrix mat, IntDoubleVector v, double[] resArr);

  void matrixNotTransSparse(BlasDoubleMatrix mat, IntFloatVector v, double[] resArr);

  void matrixNotTransSparse(BlasDoubleMatrix mat, IntLongVector v, double[] resArr);

  void matrixNotTransSparse(BlasDoubleMatrix mat, IntIntVector v, double[] resArr);

  void matrixNotTransSparse(BlasFloatMatrix mat, IntFloatVector v, float[] resArr);

  void matrixNotTransSparse(BlasFloatMatrix mat, IntLongVector v, float[] resArr);

  void matrixNotTransSparse(BlasFloatMatrix mat, IntIntVector v, float[] resArr);

  void matrixTransSorted(BlasDoubleMatrix mat, IntDoubleVector v, double[] resArr);

  void matrixTransSorted(BlasDoubleMatrix mat, IntFloatVector v, double[] resArr);

  void matrixTransSorted(BlasDoubleMatrix mat, IntLongVector v, double[] resArr);

  void matrixTransSorted(BlasDoubleMatrix mat, IntIntVector v, double[] resArr);

  void matrixTransSorted(BlasFloatMatrix mat, IntFloatVector v, float[] resArr);

  void matrixTransSorted(BlasFloatMatrix mat, IntLongVector v, float[] resArr);

  void matrixTransSorted(BlasFloatMatrix mat, IntIntVector v, float[] resArr);

  void matrixNotTransSorted(BlasDoubleMatrix mat, IntDoubleVector v, double[] resArr);

  void matrixNotTransSorted(BlasDoubleMatrix mat, IntFloatVector v, double[] resArr);

  void matrixNotTransSorted(BlasDoubleMatrix mat, IntLongVector v, double[] resArr);

  void matrixNotTransSorted(BlasDoubleMatrix mat, IntIntVector v, double[] resArr);

  void matrixNotTransSorted(BlasFloatMatrix mat, IntFloatVector v, float[] resArr);

  void matrixNotTransSorted(BlasFloatMatrix mat, IntLongVector v, float[] resArr);

  void matrixNotTransSorted(BlasFloatMatrix mat, IntIntVector v, float[] resArr);

  Matrix blasVsBlas(BlasDoubleMatrix mat1, boolean trans1, BlasDoubleMatrix mat2, boolean trans2, boolean flag);

  Matrix blasVsBlas(BlasFloatMatrix mat1, boolean trans1, BlasFloatMatrix mat2, boolean trans2, boolean flag);

  Matrix rbMatrix2BlasMatrix(RBCompIntDoubleMatrix mat1);

  Matrix rbMatrix2BlasMatrix(RBCompIntFloatMatrix mat1);
}
