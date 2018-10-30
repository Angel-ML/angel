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

import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.*;

public interface StorageOperation {
  Vector union(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector union(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector union(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector union(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector union(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector union(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector union(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector union(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector union(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector union(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector intersection(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector intersection(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector intersection(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector intersection(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector intersection(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector intersection(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector intersection(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector intersection(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector intersection(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector intersection(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector all(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector all(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector all(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector all(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector all(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector all(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector all(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector all(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector all(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector all(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);


  Vector union(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector union(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector union(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector union(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector union(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector union(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector union(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector union(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector union(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector union(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector intersection(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector intersection(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector intersection(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector intersection(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector intersection(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector intersection(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector intersection(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector intersection(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector intersection(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector intersection(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector all(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector all(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector all(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector all(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector all(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector all(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector all(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector all(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector all(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector all(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);
}
