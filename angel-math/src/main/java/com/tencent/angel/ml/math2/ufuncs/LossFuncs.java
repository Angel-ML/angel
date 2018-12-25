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

public class LossFuncs {
  public static Matrix hingeloss(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new HingeLoss(false));
  }

  public static Matrix gradhingeloss(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradHingeLoss(false));
  }

  public static Matrix logloss(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new LogLoss(false));
  }

  public static Matrix gradlogloss(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradLogLoss(false));
  }

  public static Matrix entropyloss(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new CrossEntropyLoss(false));
  }

  public static Matrix gradentropyloss(Matrix m1, Matrix m2) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradCrossEntropyLoss(false));
  }

  public static Matrix huberloss(Matrix m1, Matrix m2, double delta) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new HuberLoss(delta, false));
  }

  public static Matrix gradhuberloss(Matrix m1, Matrix m2, double delta) {
    return BinaryMatrixExecutor.apply(m1, false, m2, false, new GradHuberLoss(delta, false));
  }
}