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

import com.tencent.angel.PartitionKey;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MatrixIndex {
  private final ConcurrentHashMap<PartitionKey, PartitionIndex> partitionIndexes;

  public MatrixIndex() {
    partitionIndexes = new ConcurrentHashMap<PartitionKey, PartitionIndex>();
  }

  public ConcurrentHashMap<PartitionKey, PartitionIndex> getPartitionIndexes() {
    return partitionIndexes;
  }

  public void addPartitionIndex(PartitionKey partition, PartitionIndex index) {
    PartitionIndex oldIndex = partitionIndexes.get(partition);
    if (oldIndex == null) {
      oldIndex = partitionIndexes.putIfAbsent(partition, index);
      if (oldIndex != null) {
        oldIndex.merge(index);
      }
    }
  }

  public List<PartitionKey> getPartitions() {
    List<PartitionKey> ret = new ArrayList<PartitionKey>();
    ret.addAll(partitionIndexes.keySet());
    return ret;
  }

  public ColumnIndex getColumnIndex(int rowIndex) {
    // TODO Auto-generated method stub
    return null;
  }
}
