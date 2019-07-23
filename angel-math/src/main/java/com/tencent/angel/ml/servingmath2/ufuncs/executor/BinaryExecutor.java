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


package com.tencent.angel.ml.servingmath2.ufuncs.executor;

import com.tencent.angel.ml.servingmath2.exceptions.MathException;
import com.tencent.angel.ml.servingmath2.ufuncs.executor.simple.*;
import com.tencent.angel.ml.servingmath2.ufuncs.expression.Binary;
import com.tencent.angel.ml.servingmath2.vector.SimpleVector;
import com.tencent.angel.ml.servingmath2.vector.Vector;

public class BinaryExecutor {
  public static Vector apply(Vector v1, Vector v2, Binary op) {
    assert v1 != null && v2 != null && op != null;
    if (v1 instanceof SimpleVector && v2 instanceof SimpleVector) {
      if (op.isInplace()) {
        switch (op.getOpType()) {
          case INTERSECTION:
            return SimpleBinaryInZAExecutor.apply(v1, v2, op);
            // throw new MathException("intersection and Inplace is not supported !");
          case UNION:
            return SimpleBinaryInNonZAExecutor.apply(v1, v2, op);
          case ALL:
            return SimpleBinaryInAllExecutor.apply(v1, v2, op);
        }
      } else {
        switch (op.getOpType()) {
          case INTERSECTION:
            return SimpleBinaryOutZAExecutor.apply(v1, v2, op);
          case UNION:
            return SimpleBinaryOutNonZAExecutor.apply(v1, v2, op);
          case ALL:
            return SimpleBinaryOutAllExecutor.apply(v1, v2, op);
        }
      }
    } else {
      throw new MathException("The operation is not supported!");
    }
    return null;
  }
}