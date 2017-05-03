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

import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionUpdateIndex {
  private final ConcurrentHashMap<Integer, RowSplitUpdateIndex> rowSplitUpdateIndexes;

  public PartitionUpdateIndex() {
    rowSplitUpdateIndexes = new ConcurrentHashMap<Integer, RowSplitUpdateIndex>();
  }

  public void addUpdateIndexes(List<RowUpdateSplit> rowSplitUpdateInfos) {
    if (rowSplitUpdateInfos == null || rowSplitUpdateInfos.isEmpty()) {
      return;
    }

    int size = rowSplitUpdateInfos.size();
    for (int i = 0; i < size; i++) {
      if (!rowSplitUpdateIndexes.containsKey(rowSplitUpdateInfos.get(i).getRowId())) {
        rowSplitUpdateIndexes.putIfAbsent(rowSplitUpdateInfos.get(i).getRowId(),
            new RowSplitUpdateIndex());
      }
      RowSplitUpdateIndex rowSplitUpdateIndex =
          rowSplitUpdateIndexes.get(rowSplitUpdateInfos.get(i).getRowId());
      rowSplitUpdateIndex.addUpdateIndexes(rowSplitUpdateInfos.get(i));
    }
  }

  public RowSplitUpdateIndex getRowSplitUpdateIndex(int rowId) {
    return rowSplitUpdateIndexes.get(rowId);
  }

}
