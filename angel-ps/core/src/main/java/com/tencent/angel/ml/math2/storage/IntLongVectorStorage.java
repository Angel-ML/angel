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
import it.unimi.dsi.fastutil.longs.LongIterator;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface IntLongVectorStorage extends LongVectorStorage, IntKeyVectorStorage {
  long get(int idx);

  default long[] get(int[] idxs) {
    long[] res = new long[idxs.length];

    int out = 0;
    for (int idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(int idx, long value);

  default void set(int[] idxs, long[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Int2LongMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  IntLongVectorStorage clone();

  IntLongVectorStorage copy();

  IntLongVectorStorage oneLikeDense();

  IntLongVectorStorage oneLikeSparse();

  IntLongVectorStorage oneLikeSorted();

  IntLongVectorStorage oneLikeDense(int length);

  IntLongVectorStorage oneLikeSparse(int dim, int capacity);

  IntLongVectorStorage oneLikeSorted(int dim, int capacity);

  IntLongVectorStorage oneLikeSparse(int capacity);

  IntLongVectorStorage oneLikeSorted(int capacity);

  IntLongVectorStorage emptyDense();

  IntLongVectorStorage emptySparse();

  IntLongVectorStorage emptySorted();

  IntLongVectorStorage emptyDense(int length);

  IntLongVectorStorage emptySparse(int dim, int capacity);

  IntLongVectorStorage emptySorted(int dim, int capacity);

  IntLongVectorStorage emptySparse(int capacity);

  IntLongVectorStorage emptySorted(int capacity);
}
