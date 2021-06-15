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
package com.tencent.angel.graph.utils;

import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;

public class LongArrayListIndexComparator implements IntComparator {

  private LongArrayList array;

  public LongArrayListIndexComparator(LongArrayList array) {
    this.array = array;
  }

  @Override
  public int compare(int i, int i1) {
    if (array.getLong(i) == array.getLong(i1)) {
      return 0;
    }
    return array.getLong(i) < array.getLong(i1) ? -1 : 1;
  }

  @Override
  public int compare(Integer o1, Integer o2) {
    if (array.getLong(o1) == array.getLong(o2)) {
      return 0;
    }
    return array.getLong(o1) < array.getLong(o2) ? -1 : 1;
  }
}
