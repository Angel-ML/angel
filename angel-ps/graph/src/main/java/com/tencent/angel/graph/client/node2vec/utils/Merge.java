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
package com.tencent.angel.graph.client.node2vec.utils;

import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class Merge {

  public static <E> Long2ObjectOpenHashMap<E> mergeHadhMap(Long2ObjectOpenHashMap<E> m1,
      Long2ObjectOpenHashMap<E> m2) {
    int size = m1.size() + m2.size();

    Long2ObjectOpenHashMap<E> result = new Long2ObjectOpenHashMap<>(size);
    result.putAll(m1);
    result.putAll(m2);

    return result;
  }

  public static <E> Long2ObjectOpenHashMap<E> mergeHadhMap(List<Long2ObjectOpenHashMap<E>> ms) {
    int size = 0;

    for (Long2ObjectOpenHashMap<E> m : ms) {
      if (m != null) {
        size += m.size();
      }
    }

    Long2ObjectOpenHashMap<E> result = new Long2ObjectOpenHashMap<>(size);
    for (Long2ObjectOpenHashMap<E> m : ms) {
      if (m != null) {
        result.putAll(m);
      }
    }

    return result;
  }

  public static <E> Long2IntOpenHashMap mergeHashMap(List<Long2IntOpenHashMap> ms) {
    int size = 0;

    for (Long2IntOpenHashMap m : ms) {
      if (m != null) {
        size += m.size();
      }
    }

    Long2IntOpenHashMap result = new Long2IntOpenHashMap(size);
    for (Long2IntOpenHashMap m : ms) {
      if (m != null) {
        result.putAll(m);
      }
    }

    return result;
  }
}
