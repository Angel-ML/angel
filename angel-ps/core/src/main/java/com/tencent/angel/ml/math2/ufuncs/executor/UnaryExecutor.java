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


package com.tencent.angel.ml.math2.ufuncs.executor;


import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.ufuncs.executor.comp.CompUnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.simple.SimpleUnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.vector.*;

public class UnaryExecutor {
  public static Vector apply(Vector v1, Unary op) {
    if (v1 instanceof ComponentVector) {
      return CompUnaryExecutor.apply((ComponentVector) v1, op);
    } else if (v1 instanceof SimpleVector) {
      return SimpleUnaryExecutor.apply(v1, op);
    } else { // Dummy
      throw new AngelException("Vector Type is not support the operation!");
    }
  }
}