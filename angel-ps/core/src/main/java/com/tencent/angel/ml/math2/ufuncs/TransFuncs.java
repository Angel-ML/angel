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
import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.BinaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.matrix.UnaryMatrixExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.*;
import com.tencent.angel.ml.math2.vector.Vector;

public class TransFuncs {
  public static Vector sigmoid(Vector v1) {
    return UnaryExecutor.apply(v1, new Sigmoid(false));
  }

  public static Vector relu(Vector v1) {
    return UnaryExecutor.apply(v1, new Relu(false));
  }

  public static Vector tanh(Vector v1) {
    return UnaryExecutor.apply(v1, new Tanh(false));
  }

  public static Vector dropout(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new Dropout(false, x));
  }

  public static Vector isigmoid(Vector v1) {
    return UnaryExecutor.apply(v1, new Sigmoid(true));
  }

  public static Vector irelu(Vector v1) {
    return UnaryExecutor.apply(v1, new Relu(true));
  }

  public static Vector itanh(Vector v1) {
    return UnaryExecutor.apply(v1, new Tanh(true));
  }

  public static Vector idropout(Vector v1, double x) {
    return UnaryExecutor.apply(v1, new Dropout(true, x));
  }

  public static Matrix isigmoid(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Sigmoid(true));
  }

  public static Matrix sigmoid(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Sigmoid(false));
  }

  public static Matrix isigmoidwithdropout(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SigmoidWithDropout(true, x));
  }

  public static Matrix sigmoidwithdropout(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new SigmoidWithDropout(false, x));
  }

  public static Matrix irelu(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Relu(true));
  }

  public static Matrix relu(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Relu((false)));
  }

  public static Matrix itanh(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Tanh(true));
  }

  public static Matrix tanh(Matrix m) {
    return UnaryMatrixExecutor.apply(m, new Tanh(false));
  }

  public static Matrix itanhwithdropout(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new TanhWithDropout(true, x));
  }

  public static Matrix tanhwithdropout(Matrix m, double x) {
    return UnaryMatrixExecutor.apply(m, new TanhWithDropout(false, x));
  }

  public static Matrix idropout(Matrix m, double proba) {
    return UnaryMatrixExecutor.apply(m, new Dropout(true, proba));
  }

  public static Matrix dropout(Matrix m, double proba) {
    return UnaryMatrixExecutor.apply(m, new Dropout(false, proba));
  }

  public static Matrix igradsigmoid(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradSigmoid(true));
  }

  public static Matrix gradsigmoid(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradSigmoid(false));
  }

  public static Matrix igradsigmoidwithdropout(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradSigmoidWithDropout(true));
  }

  public static Matrix gradsigmoidwithdropout(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradSigmoidWithDropout(false));
  }

  public static Matrix igradrelu(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradRelu(true));
  }

  public static Matrix gradrelu(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradRelu(false));
  }

  public static Matrix igradtanh(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradTanh(true));
  }

  public static Matrix gradtanh(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradTanh(false));
  }

  public static Matrix igradtanhwithdropout(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradTanhWithDropout(true));
  }

  public static Matrix gradtanhwithdropout(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradTanhWithDropout(false));
  }

  public static Matrix igraddropout(Matrix m1, Matrix m2, double proba) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradDropout(true, proba));
  }

  public static Matrix graddropout(Matrix m1, Matrix m2, double proba) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradDropout(false, proba));
  }
}