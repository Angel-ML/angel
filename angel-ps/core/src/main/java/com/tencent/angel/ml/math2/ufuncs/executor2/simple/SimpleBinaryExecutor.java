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


package com.tencent.angel.ml.math2.ufuncs.executor2.simple;


import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.ufuncs.expression.OpType;
import com.tencent.angel.ml.math2.vector.Vector;


public class SimpleBinaryExecutor {
  public static Vector apply(Vector v1, Vector v2, Binary op) {
	Vector v;
	StorageOperationFactory.createStorageOperation();
	if (op.getOpType() == OpType.UNION) {
	  v = SimpleBinaryUnionExecutor.getInstance().apply(v1, v2, op);
	} else if (op.getOpType() == OpType.INTERSECTION) {
	  v = SimpleBinaryIntersectionExecutor.getInstance().apply(v1, v2, op);
	} else {
	  v = SimpleBinaryAllExecutor.getInstance().apply(v1, v2, op);
	}
	return v;
  }
}
