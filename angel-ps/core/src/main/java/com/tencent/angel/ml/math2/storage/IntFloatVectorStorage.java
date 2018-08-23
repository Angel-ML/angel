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

import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public interface IntFloatVectorStorage extends FloatVectorStorage, IntKeyVectorStorage {
  float get(int idx);

  default float[] get(int[] idxs) {
    float[] res = new float[idxs.length];

    int out = 0;
    for (int idx : idxs) {
      res[out++] = get(idx);
    }

    return res;
  }

  void set(int idx, float value);

  default void set(int[] idxs, float[] values) {
    for (int i = 0; i < idxs.length; i++) {
      set(idxs[i], values[i]);
    }
  }

  default ObjectIterator<Int2FloatMap.Entry> entryIterator() {
    throw new NotImplementedException();
  }

  IntFloatVectorStorage clone();

  IntFloatVectorStorage copy();

  IntFloatVectorStorage oneLikeDense();

  IntFloatVectorStorage oneLikeSparse();

  IntFloatVectorStorage oneLikeSorted();

  IntFloatVectorStorage oneLikeDense(int length);

  IntFloatVectorStorage oneLikeSparse(int dim, int capacity);

  IntFloatVectorStorage oneLikeSorted(int dim, int capacity);

  IntFloatVectorStorage oneLikeSparse(int capacity);

  IntFloatVectorStorage oneLikeSorted(int capacity);

  IntFloatVectorStorage emptyDense();

  IntFloatVectorStorage emptySparse();

  IntFloatVectorStorage emptySorted();

  IntFloatVectorStorage emptyDense(int length);

  IntFloatVectorStorage emptySparse(int dim, int capacity);

  IntFloatVectorStorage emptySorted(int dim, int capacity);

  IntFloatVectorStorage emptySparse(int capacity);

  IntFloatVectorStorage emptySorted(int capacity);
}
