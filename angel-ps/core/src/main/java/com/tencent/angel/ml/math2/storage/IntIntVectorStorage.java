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

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface IntIntVectorStorage extends IntVectorStorage, IntKeyVectorStorage {

  int get(int idx);

  default int[] get(int[] idxs) {
    int[] res = new int[idxs.length];

    int out = 0;
    for (int idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(int idx, int value);

  default void set(int[] idxs, int[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Int2IntMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  IntIntVectorStorage clone();

  IntIntVectorStorage copy();

  IntIntVectorStorage oneLikeDense();

  IntIntVectorStorage oneLikeSparse();

  IntIntVectorStorage oneLikeSorted();

  IntIntVectorStorage oneLikeDense(int length);

  IntIntVectorStorage oneLikeSparse(int dim, int capacity);

  IntIntVectorStorage oneLikeSorted(int dim, int capacity);

  IntIntVectorStorage oneLikeSparse(int capacity);

  IntIntVectorStorage oneLikeSorted(int capacity);

  IntIntVectorStorage emptyDense();

  IntIntVectorStorage emptySparse();

  IntIntVectorStorage emptySorted();

  IntIntVectorStorage emptyDense(int length);

  IntIntVectorStorage emptySparse(int dim, int capacity);

  IntIntVectorStorage emptySorted(int dim, int capacity);

  IntIntVectorStorage emptySparse(int capacity);

  IntIntVectorStorage emptySorted(int capacity);
}