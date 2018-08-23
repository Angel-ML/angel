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

import com.tencent.angel.ml.matrix.RowType;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface LongLongVectorStorage extends LongVectorStorage, LongKeyVectorStorage {
  long get(long idx);

  default long[] get(long[] idxs) {
    long[] res = new long[idxs.length];

    int out = 0;
    for (long idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(long idx, long value);

  default void set(long[] idxs, long[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Long2LongMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  LongLongVectorStorage clone();

  LongLongVectorStorage copy();

  LongLongVectorStorage oneLikeSparse();

  LongLongVectorStorage oneLikeSorted();

  LongLongVectorStorage oneLikeSparse(long dim, int capacity);

  LongLongVectorStorage oneLikeSorted(long dim, int capacity);

  LongLongVectorStorage oneLikeSparse(int capacity);

  LongLongVectorStorage oneLikeSorted(int capacity);

  LongLongVectorStorage emptySparse();

  LongLongVectorStorage emptySorted();

  LongLongVectorStorage emptySparse(long dim, int capacity);

  LongLongVectorStorage emptySorted(long dim, int capacity);

  LongLongVectorStorage emptySparse(int capacity);

  LongLongVectorStorage emptySorted(int capacity);
}
