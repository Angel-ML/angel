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

import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface LongIntVectorStorage extends IntVectorStorage, LongKeyVectorStorage {

  int get(long idx);

  default int[] get(long[] idxs) {
    int[] res = new int[idxs.length];

    int out = 0;
    for (long idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(long idx, int value);

  default void set(long[] idxs, int[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Long2IntMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  LongIntVectorStorage clone();

  LongIntVectorStorage copy();

  LongIntVectorStorage oneLikeSparse();

  LongIntVectorStorage oneLikeSorted();

  LongIntVectorStorage oneLikeSparse(long dim, int capacity);

  LongIntVectorStorage oneLikeSorted(long dim, int capacity);

  LongIntVectorStorage oneLikeSparse(int capacity);

  LongIntVectorStorage oneLikeSorted(int capacity);

  LongIntVectorStorage emptySparse();

  LongIntVectorStorage emptySorted();

  LongIntVectorStorage emptySparse(long dim, int capacity);

  LongIntVectorStorage emptySorted(long dim, int capacity);

  LongIntVectorStorage emptySparse(int capacity);

  LongIntVectorStorage emptySorted(int capacity);
}