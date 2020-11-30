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

import com.tencent.angel.ml.math2.ufuncs.executor.comp.CompDotExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.mixed.MixedDotExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor.simple.SimpleDotExecutor;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import com.tencent.angel.ml.math2.vector.Vector;

public class DotExecutor {

  public static double apply(Vector v1, Vector v2) {
    if (v1 instanceof ComponentVector && v2 instanceof ComponentVector) {
      try {
        return CompDotExecutor.apply((ComponentVector) v1, (ComponentVector) v2);
      } catch (Exception e) {
        return CompDotExecutor.apply((ComponentVector) v2, (ComponentVector) v1);
      }
    } else if (v1 instanceof ComponentVector || v2 instanceof ComponentVector) {
      if (v1 instanceof ComponentVector) {
        return MixedDotExecutor.apply((ComponentVector) v1, v2);
      } else {
        return MixedDotExecutor.apply((ComponentVector) v2, v1);
      }
    } else {
      try {
        return SimpleDotExecutor.apply(v1, v2);
      } catch (Exception e) {
        return SimpleDotExecutor.apply(v2, v1);
      }
    }
  }
}