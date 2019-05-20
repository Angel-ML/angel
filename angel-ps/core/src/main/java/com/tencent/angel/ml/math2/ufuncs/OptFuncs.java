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

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.BinaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.*;
import com.tencent.angel.ml.math2.vector.Vector;


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
    return iexpsmoothing2(v1, v2, factor, OpType.UNION);
  }

  public static Vector iexpsmoothing2(Vector v1, Vector v2, double factor, OpType type) {
    return BinaryExecutor.apply(v1, v2, new ExpSmoothing2(true, factor, type));
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
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new FtrlDelta(false, alpha));
  }

  public static Vector iftrldelta(Vector v1, Vector v2, double alpha) {
    return BinaryExecutor.apply(v1, v2, new FtrlDelta(true, alpha));
  }

  public static Vector ftrldelta(Vector v1, Vector v2, double alpha) {
    return ftrldelta(v1, v2, alpha, OpType.UNION);
  }

  public static Vector ftrldelta(Vector v1, Vector v2, double alpha, OpType type) {
    return BinaryExecutor.apply(v1, v2, new FtrlDelta(false, alpha, type));
  }

  public static Vector ftrldetalintersect(Vector v1, Vector v2, double alpha) {
    return BinaryExecutor.apply(v1, v2, new FtrlDeltaIntersect(false, alpha));
  }

  public static Vector iftrldetalintersect(Vector v1, Vector v2, double alpha) {
    return BinaryExecutor.apply(v1, v2, new FtrlDeltaIntersect(true, alpha));
  }

  // -----------------
  public static Matrix iadagraddelta(Matrix m1, Matrix m2, double lambda, double eta) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdaGradDelta(true, lambda, eta));
  }

  public static Matrix adagraddelta(Matrix m1, Matrix m2, double lambda, double eta) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdaGradDelta(false, lambda, eta));
  }

  public static Vector iadagraddelta(Vector v1, Vector v2, double lambda, double eta) {
    return BinaryExecutor.apply(v1, v2, new AdaGradDelta(true, lambda, eta));
  }

  public static Vector adagraddelta(Vector v1, Vector v2, double lambda, double eta) {
    return BinaryExecutor.apply(v1, v2, new AdaGradDelta(false, lambda, eta));
  }

  // -----------------
  public static Matrix iadagradthredshold(Matrix m1, Matrix m2, double lambda1, double lambda2,
      double eta) {
    return BinaryMatrixExecutor
        .apply(m1, false, m2, false, new AdaGradThreshold(true, lambda1, lambda2, eta));
  }

  public static Matrix adagradthredshold(Matrix m1, Matrix m2, double lambda1, double lambda2,
      double eta) {
    return BinaryMatrixExecutor
        .apply(m1, false, m2, false, new AdaGradThreshold(false, lambda1, lambda2, eta));
  }

  public static Vector iadagradthredshold(Vector v1, Vector v2, double lambda1, double lambda2,
      double eta) {
    return BinaryExecutor.apply(v1, v2, new AdaGradThreshold(true, lambda1, lambda2, eta));
  }

  public static Vector adagradthredshold(Vector v1, Vector v2, double lambda1, double lambda2,
      double eta) {
    return BinaryExecutor.apply(v1, v2, new AdaGradThreshold(false, lambda1, lambda2, eta));
  }

  // -----------------
  public static Matrix iadadeltahessian(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdaDeltaHessian(true));
  }

  public static Matrix adadeltahessian(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdaDeltaHessian(false));
  }

  public static Vector iadadeltahessian(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new AdaDeltaHessian(true));
  }

  public static Vector adadeltahessian(Vector v1, Vector v2) {
    return BinaryExecutor.apply(v1, v2, new AdaDeltaHessian(false));
  }

  // -----------------
  public static Matrix iadadeltadelta(Matrix m1, Matrix m2, double lambda) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdaDeltaDelta(true, lambda));
  }

  public static Matrix adadeltadelta(Matrix m1, Matrix m2, double lambda) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new AdaDeltaDelta(false, lambda));
  }

  public static Vector iadadeltadelta(Vector v1, Vector v2, double lambda) {
    return BinaryExecutor.apply(v1, v2, new AdaDeltaDelta(true, lambda));
  }

  public static Vector adadeltadelta(Vector v1, Vector v2, double lambda) {
    return BinaryExecutor.apply(v1, v2, new AdaDeltaDelta(false, lambda));
  }

  // -----------------
  public static Matrix iadadeltathredshold(Matrix m1, Matrix m2, double lambda1, double lambda2) {
    return BinaryMatrixExecutor
        .apply(m1, false, m2, false, new AdaDeltaThreshold(true, lambda1, lambda2));
  }

  public static Matrix adadeltathredshold(Matrix m1, Matrix m2, double lambda1, double lambda2) {
    return BinaryMatrixExecutor
        .apply(m1, false, m2, false, new AdaDeltaThreshold(false, lambda1, lambda2));
  }

  public static Vector iadadeltathredshold(Vector v1, Vector v2, double lambda1, double lambda2) {
    return BinaryExecutor.apply(v1, v2, new AdaDeltaThreshold(true, lambda1, lambda2));
  }

  public static Vector adadeltathredshold(Vector v1, Vector v2, double lambda1, double lambda2) {
    return BinaryExecutor.apply(v1, v2, new AdaDeltaThreshold(false, lambda1, lambda2));
  }
}