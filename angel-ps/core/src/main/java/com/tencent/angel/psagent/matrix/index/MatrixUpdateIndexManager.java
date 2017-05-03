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

import com.google.protobuf.ByteString;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class MatrixUpdateIndexManager {
  private final ConcurrentHashMap<PartitionKey, PartitionUpdateIndex> partitionUpdateIndexes;

  public MatrixUpdateIndexManager() {
    partitionUpdateIndexes = new ConcurrentHashMap<PartitionKey, PartitionUpdateIndex>();
  }

  public void addUpdateIndexes(Collection<Map<PartitionKey, List<RowUpdateSplit>>> updateInfos) {
    if (updateInfos == null || updateInfos.isEmpty()) {
      return;
    }

    for (Map<PartitionKey, List<RowUpdateSplit>> partUpdateInfoMap : updateInfos) {
      for (Entry<PartitionKey, List<RowUpdateSplit>> partUpdateInfo : partUpdateInfoMap.entrySet()) {
        if (!partitionUpdateIndexes.contains(partUpdateInfo.getKey())) {
          partitionUpdateIndexes.putIfAbsent(partUpdateInfo.getKey(), new PartitionUpdateIndex());
        }

        PartitionUpdateIndex partitionUpdateIndex =
            partitionUpdateIndexes.get(partUpdateInfo.getKey());
        partitionUpdateIndex.addUpdateIndexes(partUpdateInfo.getValue());
      }
    }
  }

  public ConcurrentHashMap<PartitionKey, PartitionUpdateIndex> getPartitionUpdateIndexes() {
    return partitionUpdateIndexes;
  }

  public PartitionUpdateIndex getPartitionUpdateIndex(PartitionKey partitionKey) {
    return partitionUpdateIndexes.get(partitionKey);
  }

  public int[] getRowSplitIndexes(PartitionKey partitionKey, int rowId) {
    return partitionUpdateIndexes.get(partitionKey).getRowSplitUpdateIndex(rowId).getIndexes();
  }

  public ByteString getSerilizedRowSplitIndexes(PartitionKey partitionKey, int rowId) {
    return partitionUpdateIndexes.get(partitionKey).getRowSplitUpdateIndex(rowId)
        .getSerilizedIndexes();
  }

}
