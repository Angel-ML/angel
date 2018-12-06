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


package com.tencent.angel.ml.math2.ufuncs.executor.comp;

import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.utils.UnionEle;
import com.tencent.angel.ml.math2.vector.ComponentVector;

public interface CompExecutor {

  void apply(ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result, int start, int end);

  void apply(ComponentVector v, Unary op, ComponentVector result, int start, int end);

  double apply(ComponentVector v1, ComponentVector v2, int start, int end);

  UnionEle apply(ComponentVector v, CompReduceExecutor.ReduceOP op, int start, int end);
}
