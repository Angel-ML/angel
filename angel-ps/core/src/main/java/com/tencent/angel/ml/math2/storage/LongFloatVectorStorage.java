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
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface LongFloatVectorStorage extends FloatVectorStorage, LongKeyVectorStorage {
  float get(long idx);

  default float[] get(long[] idxs) {
    float[] res = new float[idxs.length];

    int out = 0;
    for (long idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(long idx, float value);

  default void set(long[] idxs, float[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Long2FloatMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  LongFloatVectorStorage clone();

  LongFloatVectorStorage copy();

  LongFloatVectorStorage oneLikeSparse();

  LongFloatVectorStorage oneLikeSorted();

  LongFloatVectorStorage oneLikeSparse(long dim, int capacity);

  LongFloatVectorStorage oneLikeSorted(long dim, int capacity);

  LongFloatVectorStorage oneLikeSparse(int capacity);

  LongFloatVectorStorage oneLikeSorted(int capacity);

  LongFloatVectorStorage emptySparse();

  LongFloatVectorStorage emptySorted();

  LongFloatVectorStorage emptySparse(long dim, int capacity);

  LongFloatVectorStorage emptySorted(long dim, int capacity);

  LongFloatVectorStorage emptySparse(int capacity);

  LongFloatVectorStorage emptySorted(int capacity);
}
