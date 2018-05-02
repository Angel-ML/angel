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

package com.tencent.angel.psagent.matrix.cache;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache for a single matrix.
 */
public class MatrixCache {
  /** matrix id */
  private final int matrixId;
  
  /**partition key to partition map*/
  private final ConcurrentHashMap<PartitionKey, ServerPartition> partitionCacheMap;

  /**
   * Create a new MatrixCache.
   *
   * @param matrixId matrix id
   */
  public MatrixCache(int matrixId) {
    this.matrixId = matrixId;
    this.partitionCacheMap = new ConcurrentHashMap<PartitionKey, ServerPartition>();
  }

  /**
   * Get a partition from cache
   * 
   * @param partKey partition key
   * @return matrix partition
   */
  public ServerPartition getPartition(PartitionKey partKey) {
    return partitionCacheMap.get(partKey);
  }

  /**
   * Get a row split from cache
   * 
   * @param partKey partition key
   * @param rowIndex row index
   * @return the row split
   */
  public ServerRow getRowSplit(PartitionKey partKey, int rowIndex) {
    ServerPartition partCache = partitionCacheMap.get(partKey);
    if (partCache == null) {
      return null;
    }
    return partCache.getRow(rowIndex);
  }

  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Get a batch of row splits that belong to a matrix partition
   * 
   * @param partKey partition key
   * @param rowIndexes row indexes
   * @return a batch of row splits
   */
  public List<ServerRow> getRowsSplit(PartitionKey partKey, List<Integer> rowIndexes) {
    ServerPartition partCache = partitionCacheMap.get(partKey);
    if (partCache == null) {
      return null;
    }
    return partCache.getRows(rowIndexes);
  }

  public ConcurrentHashMap<PartitionKey, ServerPartition> getPartitionCacheMap() {
    return partitionCacheMap;
  }

  /**
   * Update a matrix partition in the cache
   * 
   * @param partKey partition key
   * @param part matrix partition
   */
  public void update(PartitionKey partKey, ServerPartition part) {
    ServerPartition partCache = partitionCacheMap.get(partKey);
    if (partCache == null || partCache.getClock() <= part.getClock()) {
      partitionCacheMap.put(partKey, part);
    }
  }

  /**
   * Update a row split in the cache
   * 
   * @param partKey partition key
   * @param rowSplit row split
   */
  public void update(PartitionKey partKey, ServerRow rowSplit) {
    ServerPartition partCache = partitionCacheMap.get(partKey);
    if (partCache == null) {
      partitionCacheMap.putIfAbsent(partKey, new ServerPartition(partKey,
        PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getRowType(),0));
      partCache = partitionCacheMap.get(partKey);
    }
    
    partCache.update(rowSplit);
  }

  /**
   * Update a batch row splits in the cache
   * 
   * @param partKey partition key
   * @param rowsSplit a batch row splits
   */
  public void update(PartitionKey partKey, List<ServerRow> rowsSplit) {
    ServerPartition partCache = partitionCacheMap.get(partKey);
    if (partCache == null) {
      partitionCacheMap.putIfAbsent(partKey, new ServerPartition(partKey,
        PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getRowType(),0));
      partCache = partitionCacheMap.get(partKey);
    }
    
    partCache.update(rowsSplit);
  }

  /**
   * Clean a matrix partition from cache
   * 
   * @param partitionKey partition key
   */  
  public void clear(PartitionKey partitionKey) {
    partitionCacheMap.remove(partitionKey);
  }
}
