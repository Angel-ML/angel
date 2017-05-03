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
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache sync policy using matrix partition clock, it will pre-fetch a matrix partition from ps if
 * the clock of the matrix partition is updated.
 */
public class PartitionClockPolicy implements SyncPolicy {
  private static final Log LOG = LogFactory.getLog(PartitionClockPolicy.class);

  @Override
  public void sync(MatricesCache cache) {
    ClockCache clockCache = PSAgentContext.get().getClockCache();
    MatrixTransportClient matrixClient = PSAgentContext.get().getMatrixTransportClient();
    ConcurrentHashMap<Integer, MatrixCache> matrixCacheMap = cache.getMatricesCacheMap();
    PartitionKey partKey = null;
    
    for (Entry<Integer, MatrixCache> matrixCacheEntry : matrixCacheMap.entrySet()) {
      ConcurrentHashMap<PartitionKey, ServerPartition> partitionCacheMap =
          matrixCacheEntry.getValue().getPartitionCacheMap();

      for (Entry<PartitionKey, ServerPartition> partitionCacheEntry : partitionCacheMap.entrySet()) {
        partKey = partitionCacheEntry.getKey();
        int cachedClock =
            clockCache.getClock(matrixCacheEntry.getKey(), partKey);
        
        if (partitionCacheEntry.getValue().getClock() < cachedClock) {
          LOG.debug("start to prefetch partition " + partKey + " now");
          try {
            if(partKey.getEndRow() - partKey.getStartRow() == 1){
              matrixClient.getRowSplit(partKey, partKey.getStartRow(), cachedClock);
            }else{
              matrixClient.getPart(partKey, cachedClock);
            }       
          } catch (Exception e) {
            LOG.error("get partition " + partKey + " failed, ", e);
          }
        }
      }
    }
  }
}
