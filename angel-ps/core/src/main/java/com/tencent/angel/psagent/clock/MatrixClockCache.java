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

package com.tencent.angel.psagent.clock;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The partition clocks cache for a matrix
 */
public class MatrixClockCache {
  /**matrix id*/
  private final int matrixId;
  
  /**matrix partition key to clock map*/
  private final ConcurrentHashMap<PartitionKey, Integer> partitionClockMap;

  /**
   * Create a new MatrixClockCache instance.
   *
   * @param matrixId matrix id
   * @param partitions matrix partitions
   */
  public MatrixClockCache(int matrixId, List<PartitionKey> partitions) {
    this.matrixId = matrixId;
    partitionClockMap = new ConcurrentHashMap<PartitionKey, Integer>();
    int size = partitions.size();
    for (int i = 0; i < size; i++) {
      partitionClockMap.put(partitions.get(i), 0);
    }
  }

  /**
   * Create a new MatrixClockCache instance.
   *
   * @param matrixId matrix id
   */
  public MatrixClockCache(int matrixId) {
    this(matrixId, new ArrayList<PartitionKey>());
  }

  /**
   * Get minimal clock of the given partitions
   * 
   * @param parts partitions
   * @return int minimal clock of the given partitions
   */
  public int getPartitionsClock(List<PartitionKey> parts) {
    int size = parts.size();
    int minClock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      int clock = partitionClockMap.get(parts.get(i));
      if (clock < minClock) {
        minClock = clock;
      }
    }

    return minClock;
  }


  /**
   * Get a partition clock
   * 
   * @param partKey partition key
   * @return int clock
   */
  public int getClock(PartitionKey partKey) {
    if (partitionClockMap.containsKey(partKey)) {
      return partitionClockMap.get(partKey);
    } else {
      return 0;
    }
  }

  /**
   * Update clock of a partition
   * 
   * @param partKey partition key
   * @param clock clock value
   */
  public void update(PartitionKey partKey, int clock) {
    partitionClockMap.put(partKey, clock);
  }

  /**
   * Get minimal clock of the given row
   * 
   * @param rowIndex row index
   * @return int minimal clock of the given row
   */
  public int getClock(int rowIndex) {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowIndex);
    int size = parts.size();
    int clock = Integer.MAX_VALUE;
    for (int i = 0; i < size; i++) {
      int partClock = getClock(parts.get(i));
      if (partClock < clock) {
        clock = partClock;
      }
    }

    return clock;
  }
}
