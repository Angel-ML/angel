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


package com.tencent.angel.ml.math2.ufuncs;

import com.tencent.angel.ml.math2.ufuncs.expression.AdamDelta;
import com.tencent.angel.ml.math2.ufuncs.expression.FtrlDelta;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.BinaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.ExpSmoothing;
import com.tencent.angel.ml.math2.ufuncs.expression.ExpSmoothing2;


public class OptFuncs {
  public static Matrix iexpsmoothing(Matrix m1, Matrix m2, double factor) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new ExpSmoothing(true, factor));
  }

  public static Matrix expsmoothing(Matrix m1, Matrix m2, double factor) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new ExpSmoothing(false, factor));
  }

  public static Vector iexpsmoothing(Vector v1, Vector v2, double factor) {
    return BinaryExecutor.apply(v1, v2, new ExpSmoothing(true, factor));
  }

  public static Vector expsmoothing(Vector v1, Vector v2, double factor) {
    return BinaryExecutor.apply(v1, v2, new ExpSmoothing(false, factor));
  }

  public static Matrix iexpsmoothing2(Matrix m1, Matrix m2, double factor) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new ExpSmoothing2(true, factor));
  }

  public static Matrix expsmoothing2(Matrix m1, Matrix m2, double factor) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new ExpSmoothing2(false, factor));
  }

  public static Vector iexpsmoothing2(Vector v1, Vector v2, double factor) {
    return BinaryExecutor.apply(v1, v2, new ExpSmoothing2(true, factor));
  }

  public static Vector expsmoothing2(Vector v1, Vector v2, double factor) {
    return BinaryExecutor.apply(v1, v2, new ExpSmoothing2(false, factor));
  }

  public static Matrix iadamdelta(Matrix m1, Matrix m2, double powBeta, double powGamma) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdamDelta(true, powBeta, powGamma));
  }

  public static Matrix adamdelta(Matrix m1, Matrix m2, double powBeta, double powGamma) {
    return BinaryMatrixExecutor
      .apply(m1, false, m2, false, new AdamDelta(false, powBeta, powGamma));
  }

  public static Vector iadamdelta(Vector v1, Vector v2, double powBeta, double powGamma) {
    return BinaryExecutor.apply(v1, v2, new AdamDelta(true, powBeta, powGamma));
  }

  public static Vector adamdelta(Vector v1, Vector v2, double powBeta, double powGamma) {
    return BinaryExecutor.apply(v1, v2, new AdamDelta(false, powBeta, powGamma));
  }

  public static Matrix iftrldelta(Matrix m1, Matrix m2, double alpha) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new FtrlDelta(true, alpha));
  }

  public static Matrix ftrldelta(Matrix m1, Matrix m2, double alpha) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new FtrlDelta(true, alpha));
  }

  public static Vector iftrldelta(Vector v1, Vector v2, double alpha) {
    return BinaryExecutor.apply(v1, v2, new FtrlDelta(true, alpha));
  }

  public static Vector ftrldelta(Vector v1, Vector v2, double alpha) {
    return BinaryExecutor.apply(v1, v2, new FtrlDelta(true, alpha));
  }
}