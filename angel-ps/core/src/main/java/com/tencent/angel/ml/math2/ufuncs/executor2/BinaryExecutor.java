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


package com.tencent.angel.ml.math2.ufuncs.executor2;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.ufuncs.executor2.comp.CompBinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor2.mixed.MixedBinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.executor2.simple.SimpleBinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import com.tencent.angel.ml.math2.vector.SimpleVector;
import com.tencent.angel.ml.math2.vector.Vector;

public class BinaryExecutor {
  public static Vector apply(Vector v1, Vector v2, Binary op) {
	assert v1 != null && v2 != null && op != null;
	if (v1 instanceof ComponentVector && v2 instanceof ComponentVector) {
	  return CompBinaryExecutor.apply((ComponentVector) v1, (ComponentVector) v2, op);
	} else if (v1 instanceof ComponentVector && v2 instanceof SimpleVector) {
	  return MixedBinaryExecutor.apply(v1, v2, op);
	} else if (v1 instanceof SimpleVector && v2 instanceof SimpleVector) {
	  return SimpleBinaryExecutor.apply(v1, v2, op);
	} else {
	  throw new AngelException("The operation is not supported!");
	}
  }
}