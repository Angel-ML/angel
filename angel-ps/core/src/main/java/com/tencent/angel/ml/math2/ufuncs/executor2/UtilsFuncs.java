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

import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.*;

public interface UtilsFuncs {

  void rightZeroOperation(IntDoubleVector v1, IntDoubleVectorStorage newStorage, Binary op);

  void rightZeroOperation(IntFloatVector v1, IntFloatVectorStorage newStorage, Binary op);

  void rightZeroOperation(IntLongVector v1, IntLongVectorStorage newStorage, Binary op);

  void rightZeroOperation(IntIntVector v1, IntIntVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  void leftZeroOperation(IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  void replaceOperation(IntDoubleVector v1, IntDoubleVectorStorage newStorage);

  void replaceOperation(IntFloatVector v1, IntFloatVectorStorage newStorage);

  void replaceOperation(IntLongVector v1, IntLongVectorStorage newStorage);

  void replaceOperation(IntIntVector v1, IntIntVectorStorage newStorage);

  Vector otherVsDense(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsDense(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsSparse(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsSorted(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector denseVsDense(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sparseVsDense(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsDense(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector allSparse(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector allSparse(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector allSparse(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector allSparse(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector allSparse(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector allSparse(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntDoubleVector v1, IntDoubleVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntDoubleVector v1, IntFloatVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntDoubleVector v1, IntLongVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntDoubleVector v1, IntIntVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntFloatVector v1, IntFloatVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntFloatVector v1, IntLongVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntFloatVector v1, IntIntVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntLongVector v1, IntLongVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntLongVector v1, IntIntVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntIntVector v1, IntIntVector v2, IntIntVectorStorage newStorage, Binary op);


  void rightZeroOperation(LongDoubleVector v1, LongDoubleVectorStorage newStorage, Binary op);

  void rightZeroOperation(LongFloatVector v1, LongFloatVectorStorage newStorage, Binary op);

  void rightZeroOperation(LongLongVector v1, LongLongVectorStorage newStorage, Binary op);

  void rightZeroOperation(LongIntVector v1, LongIntVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  void leftZeroOperation(LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  void replaceOperation(LongDoubleVector v1, LongDoubleVectorStorage newStorage);

  void replaceOperation(LongFloatVector v1, LongFloatVectorStorage newStorage);

  void replaceOperation(LongLongVector v1, LongLongVectorStorage newStorage);

  void replaceOperation(LongIntVector v1, LongIntVectorStorage newStorage);

  Vector otherVsDense(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsDense(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsSparse(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsSorted(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsDenseFaster(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse1(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSparse2(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted1(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsSorted2(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector allSparse(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector allSparse(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector allSparse(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector allSparse(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector allSparse(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector allSparse(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongDoubleVector v1, LongDoubleVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongDoubleVector v1, LongFloatVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongDoubleVector v1, LongLongVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongDoubleVector v1, LongIntVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongFloatVector v1, LongFloatVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongFloatVector v1, LongLongVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongFloatVector v1, LongIntVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongLongVector v1, LongLongVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongLongVector v1, LongIntVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongIntVector v1, LongIntVector v2, LongIntVectorStorage newStorage, Binary op);


  Vector otherVsDummy(IntDoubleVector v1, IntDummyVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDummy(IntFloatVector v1, IntDummyVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector otherVsDummy(IntLongVector v1, IntDummyVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector otherVsDummy(IntIntVector v1, IntDummyVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(IntDoubleVector v1, IntDummyVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(IntFloatVector v1, IntDummyVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(IntLongVector v1, IntDummyVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(IntIntVector v1, IntDummyVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(IntDoubleVector v1, IntDummyVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(IntFloatVector v1, IntDummyVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(IntLongVector v1, IntDummyVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(IntIntVector v1, IntDummyVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntDoubleVector v1, IntDummyVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntFloatVector v1, IntDummyVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntLongVector v1, IntDummyVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(IntIntVector v1, IntDummyVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector allSparse(IntDoubleVector v1, IntDummyVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(IntFloatVector v1, IntDummyVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector allSparse(IntLongVector v1, IntDummyVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector allSparse(IntIntVector v1, IntDummyVector v2, IntIntVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntDoubleVector v1, IntDummyVector v2, IntDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntFloatVector v1, IntDummyVector v2, IntFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntLongVector v1, IntDummyVector v2, IntLongVectorStorage newStorage, Binary op);

  Vector sparseVsOther(IntIntVector v1, IntDummyVector v2, IntIntVectorStorage newStorage, Binary op);


  Vector otherVsDummy(LongDoubleVector v1, LongDummyVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector otherVsDummy(LongFloatVector v1, LongDummyVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector otherVsDummy(LongLongVector v1, LongDummyVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector otherVsDummy(LongIntVector v1, LongDummyVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(LongDoubleVector v1, LongDummyVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(LongFloatVector v1, LongDummyVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(LongLongVector v1, LongDummyVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsDummy1(LongIntVector v1, LongDummyVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(LongDoubleVector v1, LongDummyVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(LongFloatVector v1, LongDummyVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(LongLongVector v1, LongDummyVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sortedVsDummy2(LongIntVector v1, LongDummyVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongDoubleVector v1, LongDummyVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongFloatVector v1, LongDummyVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongLongVector v1, LongDummyVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector denseVsOtherFaster(LongIntVector v1, LongDummyVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector allSparse(LongDoubleVector v1, LongDummyVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector allSparse(LongFloatVector v1, LongDummyVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector allSparse(LongLongVector v1, LongDummyVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector allSparse(LongIntVector v1, LongDummyVector v2, LongIntVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongDoubleVector v1, LongDummyVector v2, LongDoubleVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongFloatVector v1, LongDummyVector v2, LongFloatVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongLongVector v1, LongDummyVector v2, LongLongVectorStorage newStorage, Binary op);

  Vector sparseVsOther(LongIntVector v1, LongDummyVector v2, LongIntVectorStorage newStorage, Binary op);


  void CompDenseVsOther(IntDoubleVector part, IntDoubleVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntDoubleVector part, IntFloatVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntDoubleVector part, IntLongVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntDoubleVector part, IntIntVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntFloatVector part, IntFloatVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntFloatVector part, IntLongVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntFloatVector part, IntIntVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntLongVector part, IntLongVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntLongVector part, IntIntVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(IntIntVector part, IntIntVector v2, IntIntVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntDoubleVector part, IntDoubleVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntDoubleVector part, IntFloatVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntDoubleVector part, IntLongVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntDoubleVector part, IntIntVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntFloatVector part, IntFloatVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntFloatVector part, IntLongVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntFloatVector part, IntIntVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntLongVector part, IntLongVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntLongVector part, IntIntVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompLeftZeroOperation(IntIntVector part, IntIntVector v2, IntIntVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntDoubleVector part, IntDoubleVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntDoubleVector part, IntFloatVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntDoubleVector part, IntLongVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntDoubleVector part, IntIntVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntFloatVector part, IntFloatVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntFloatVector part, IntLongVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntFloatVector part, IntIntVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntLongVector part, IntLongVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntLongVector part, IntIntVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(IntIntVector part, IntIntVector v2, IntIntVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntDoubleVector part, IntDoubleVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntDoubleVector part, IntFloatVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntDoubleVector part, IntLongVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntDoubleVector part, IntIntVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntFloatVector part, IntFloatVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntFloatVector part, IntLongVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntFloatVector part, IntIntVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntLongVector part, IntLongVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntLongVector part, IntIntVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompOtherVsDense(IntIntVector part, IntIntVector v2, IntIntVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntDoubleVector part, IntDoubleVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntDoubleVector part, IntFloatVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntDoubleVector part, IntLongVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntDoubleVector part, IntIntVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntFloatVector part, IntFloatVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntFloatVector part, IntLongVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntFloatVector part, IntIntVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntLongVector part, IntLongVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntLongVector part, IntIntVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(IntIntVector part, IntIntVector v2, IntIntVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntDoubleVector part, IntDoubleVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntDoubleVector part, IntFloatVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntDoubleVector part, IntLongVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntDoubleVector part, IntIntVector v2, IntDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntFloatVector part, IntFloatVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntFloatVector part, IntLongVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntFloatVector part, IntIntVector v2, IntFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntLongVector part, IntLongVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntLongVector part, IntIntVector v2, IntLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(IntIntVector part, IntIntVector v2, IntIntVectorStorage resPartStorage, int base, Binary op);


  void CompDenseVsDummy(IntDoubleVector part, IntDummyVector v2, IntDoubleVectorStorage resPart, int base, Binary op);

  void CompDenseVsDummy(IntFloatVector part, IntDummyVector v2, IntFloatVectorStorage resPart, int base, Binary op);

  void CompDenseVsDummy(IntLongVector part, IntDummyVector v2, IntLongVectorStorage resPart, int base, Binary op);

  void CompDenseVsDummy(IntIntVector part, IntDummyVector v2, IntIntVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(IntDoubleVector part, IntDummyVector v2, IntDoubleVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(IntFloatVector part, IntDummyVector v2, IntFloatVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(IntLongVector part, IntDummyVector v2, IntLongVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(IntIntVector part, IntDummyVector v2, IntIntVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(IntDoubleVector part, IntDummyVector v2, IntDoubleVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(IntFloatVector part, IntDummyVector v2, IntFloatVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(IntLongVector part, IntDummyVector v2, IntLongVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(IntIntVector part, IntDummyVector v2, IntIntVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(IntDoubleVector part, IntDummyVector v2, IntDoubleVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(IntFloatVector part, IntDummyVector v2, IntFloatVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(IntLongVector part, IntDummyVector v2, IntLongVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(IntIntVector part, IntDummyVector v2, IntIntVectorStorage resPart, int base, Binary op);

  void CompOtherVsSparse(CompIntDoubleVector v1, IntDoubleVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntDoubleVector v1, IntFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntDoubleVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntDoubleVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntFloatVector v1, IntFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntFloatVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntFloatVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntLongVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntLongVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompIntIntVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntDoubleVector v1, IntDoubleVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntDoubleVector v1, IntFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntDoubleVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntDoubleVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntFloatVector v1, IntFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntFloatVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntFloatVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntLongVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntLongVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompIntIntVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompIntDoubleVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompIntFloatVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompIntLongVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompIntIntVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntDoubleVector v1, IntDoubleVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntDoubleVector v1, IntFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntDoubleVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntDoubleVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntFloatVector v1, IntFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntFloatVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntFloatVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntLongVector v1, IntLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntLongVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntIntVector v1, IntIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntDoubleVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntFloatVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntLongVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompIntIntVector v1, IntDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompTransStorage(CompIntDoubleVector v1, Storage[] resPartStorages);

  void CompTransStorage(CompIntFloatVector v1, Storage[] resPartStorages);

  void CompTransStorage(CompIntLongVector v1, Storage[] resPartStorages);

  void CompTransStorage(CompIntIntVector v1, Storage[] resPartStorages);

  Vector CompBuildCompVector(CompIntDoubleVector v1, Storage[] resPartStorages, Binary op);

  Vector CompBuildCompVector(CompIntFloatVector v1, Storage[] resPartStorages, Binary op);

  Vector CompBuildCompVector(CompIntLongVector v1, Storage[] resPartStorages, Binary op);

  Vector CompBuildCompVector(CompIntIntVector v1, Storage[] resPartStorages, Binary op);


  void CompDenseVsOther(LongDoubleVector part, LongDoubleVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongDoubleVector part, LongFloatVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongDoubleVector part, LongLongVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongDoubleVector part, LongIntVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongFloatVector part, LongFloatVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongFloatVector part, LongLongVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongFloatVector part, LongIntVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongLongVector part, LongLongVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongLongVector part, LongIntVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompDenseVsOther(LongIntVector part, LongIntVector v2, LongIntVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongDoubleVector part, LongDoubleVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongDoubleVector part, LongFloatVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongDoubleVector part, LongLongVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongDoubleVector part, LongIntVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongFloatVector part, LongFloatVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongFloatVector part, LongLongVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongFloatVector part, LongIntVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongLongVector part, LongLongVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongLongVector part, LongIntVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongIntVector part, LongIntVector v2, LongIntVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongDoubleVector part, LongDoubleVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongDoubleVector part, LongFloatVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongDoubleVector part, LongLongVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongDoubleVector part, LongIntVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongFloatVector part, LongFloatVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongFloatVector part, LongLongVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongFloatVector part, LongIntVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongLongVector part, LongLongVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongLongVector part, LongIntVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther1(LongIntVector part, LongIntVector v2, LongIntVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongDoubleVector part, LongDoubleVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongDoubleVector part, LongFloatVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongDoubleVector part, LongLongVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongDoubleVector part, LongIntVector v2, LongDoubleVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongFloatVector part, LongFloatVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongFloatVector part, LongLongVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongFloatVector part, LongIntVector v2, LongFloatVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongLongVector part, LongLongVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongLongVector part, LongIntVector v2, LongLongVectorStorage resPartStorage, int base, Binary op);

  void CompSortedVsOther(LongIntVector part, LongIntVector v2, LongIntVectorStorage resPartStorage, int base, Binary op);

  void CompSparseVsOther(LongDoubleVector part, LongDummyVector v2, LongDoubleVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(LongFloatVector part, LongDummyVector v2, LongFloatVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(LongLongVector part, LongDummyVector v2, LongLongVectorStorage resPart, int base, Binary op);

  void CompSparseVsOther(LongIntVector part, LongDummyVector v2, LongIntVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(LongDoubleVector part, LongDummyVector v2, LongDoubleVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(LongFloatVector part, LongDummyVector v2, LongFloatVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(LongLongVector part, LongDummyVector v2, LongLongVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther(LongIntVector part, LongDummyVector v2, LongIntVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(LongDoubleVector part, LongDummyVector v2, LongDoubleVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(LongFloatVector part, LongDummyVector v2, LongFloatVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(LongLongVector part, LongDummyVector v2, LongLongVectorStorage resPart, int base, Binary op);

  void CompSortedVsOther1(LongIntVector part, LongDummyVector v2, LongIntVectorStorage resPart, int base, Binary op);

  void CompOtherVsSparse(CompLongDoubleVector v1, LongDoubleVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongDoubleVector v1, LongFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongDoubleVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongDoubleVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongFloatVector v1, LongFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongFloatVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongFloatVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongLongVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongLongVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSparse(CompLongIntVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongDoubleVector v1, LongDoubleVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongDoubleVector v1, LongFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongDoubleVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongDoubleVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongFloatVector v1, LongFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongFloatVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongFloatVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongLongVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongLongVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSorted(CompLongIntVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompLongDoubleVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompLongFloatVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompLongLongVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsDummy(CompLongIntVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);


  void CompOtherVsSortedAll(CompLongDoubleVector v1, LongDoubleVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongDoubleVector v1, LongFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongDoubleVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongDoubleVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongFloatVector v1, LongFloatVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongFloatVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongFloatVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongLongVector v1, LongLongVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongLongVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongIntVector v1, LongIntVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongDoubleVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongFloatVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongLongVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);

  void CompOtherVsSortedAll(CompLongIntVector v1, LongDummyVector v2, Storage[] resPartStorages, Binary op);


  void CompTransStorage(CompLongDoubleVector v1, Storage[] resPartStorages);

  void CompTransStorage(CompLongFloatVector v1, Storage[] resPartStorages);

  void CompTransStorage(CompLongLongVector v1, Storage[] resPartStorages);

  void CompTransStorage(CompLongIntVector v1, Storage[] resPartStorages);

  Vector CompBuildCompVector(CompLongDoubleVector v1, Storage[] resPartStorages, Binary op);

  Vector CompBuildCompVector(CompLongFloatVector v1, Storage[] resPartStorages, Binary op);

  Vector CompBuildCompVector(CompLongLongVector v1, Storage[] resPartStorages, Binary op);

  Vector CompBuildCompVector(CompLongIntVector v1, Storage[] resPartStorages, Binary op);


}
