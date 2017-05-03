/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.psagent.matrix.index;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionIndex {
  private final ConcurrentHashMap<Integer, ColumnIndex> rowIndexes;

  public PartitionIndex() {
    this.rowIndexes = new ConcurrentHashMap<Integer, ColumnIndex>();
  }

  public void addColumnIndex(int rowIndex, ColumnIndex index) {
    ColumnIndex oldIndex = rowIndexes.get(rowIndex);
    if (oldIndex == null) {
      oldIndex = rowIndexes.putIfAbsent(rowIndex, index);
      if (oldIndex != null) {
        index.merge(oldIndex);
      }
    }
  }

  public void merge(PartitionIndex index) {
    for (Entry<Integer, ColumnIndex> rowIndex : index.rowIndexes.entrySet()) {
      addColumnIndex(rowIndex.getKey(), rowIndex.getValue());
    }
  }
}
