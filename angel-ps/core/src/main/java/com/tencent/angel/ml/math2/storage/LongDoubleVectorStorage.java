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
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.LongSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface LongDoubleVectorStorage extends DoubleVectorStorage, LongKeyVectorStorage {
  double get(long idx);

  default double[] get(long[] idxs) {
    double[] res = new double[idxs.length];

    int out = 0;
    for (long idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(long idx, double value);

  default void set(long[] idxs, double[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Long2DoubleMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  LongDoubleVectorStorage clone();

  LongDoubleVectorStorage copy();

  LongDoubleVectorStorage oneLikeSparse();

  LongDoubleVectorStorage oneLikeSorted();

  LongDoubleVectorStorage oneLikeSparse(long dim, int capacity);

  LongDoubleVectorStorage oneLikeSorted(long dim, int capacity);

  LongDoubleVectorStorage oneLikeSparse(int capacity);

  LongDoubleVectorStorage oneLikeSorted(int capacity);

  LongDoubleVectorStorage emptySparse();

  LongDoubleVectorStorage emptySorted();

  LongDoubleVectorStorage emptySparse(long dim, int capacity);

  LongDoubleVectorStorage emptySorted(long dim, int length);

  LongDoubleVectorStorage emptySparse(int capacity);

  LongDoubleVectorStorage emptySorted(int capacity);
}
