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


package com.tencent.angel.ml.math2.storage;

import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface IntDoubleVectorStorage extends DoubleVectorStorage, IntKeyVectorStorage {

  double get(int idx);

  default double[] get(int[] idxs) {
    double[] res = new double[idxs.length];

    int out = 0;
    for (int idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(int idx, double value);

  default void set(int[] idxs, double[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Int2DoubleMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  IntDoubleVectorStorage clone();

  IntDoubleVectorStorage copy();

  IntDoubleVectorStorage oneLikeDense();

  IntDoubleVectorStorage oneLikeSparse();

  IntDoubleVectorStorage oneLikeSorted();

  IntDoubleVectorStorage oneLikeDense(int length);

  IntDoubleVectorStorage oneLikeSparse(int dim, int capacity);

  IntDoubleVectorStorage oneLikeSorted(int dim, int capacity);

  IntDoubleVectorStorage oneLikeSparse(int capacity);

  IntDoubleVectorStorage oneLikeSorted(int capacity);

  IntDoubleVectorStorage emptyDense();

  IntDoubleVectorStorage emptySparse();

  IntDoubleVectorStorage emptySorted();

  IntDoubleVectorStorage emptyDense(int length);

  IntDoubleVectorStorage emptySparse(int dim, int capacity);

  IntDoubleVectorStorage emptySorted(int dim, int capacity);

  IntDoubleVectorStorage emptySparse(int capacity);

  IntDoubleVectorStorage emptySorted(int capacity);
}