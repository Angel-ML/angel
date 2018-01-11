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
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.transport.GetClocksResponse;
import com.tencent.angel.ml.matrix.transport.ResponseType;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.MatrixTransportInterface;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The partition clocks cache for all matrices.
 */
public class ClockCache {
  private static final Log LOG = LogFactory.getLog(ClockCache.class);
  /**matrix id to matrix clock cache map*/
  private final ConcurrentHashMap<Integer, MatrixClockCache> matrixClockCacheMap;
  
  /**clocks sync thread*/
  private Syncer syncer;
  
  /**clocks sync time interval in milliseconds*/
  private int syncTimeIntervalMS;
  
  /**stop the sync thread*/
  private final AtomicBoolean stopped;

  public ClockCache() {
    matrixClockCacheMap = new ConcurrentHashMap<Integer, MatrixClockCache>();
    stopped = new AtomicBoolean(false);
  }

  /**
   * Start sync thread
   */
  public void start() {
    syncTimeIntervalMS =
        PSAgentContext
            .get()
            .getConf()
            .getInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS,
                AngelConf.DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS);

    syncer = new Syncer();
    syncer.setName("clock-syncer");
    syncer.start();
  }

  /**
   * Stop sync thread
   */
  public void stop() {
    if(!stopped.getAndSet(true)){
      if (syncer != null) {
        syncer.interrupt();
      }
      matrixClockCacheMap.clear();
    }
  }

  /**
   * Remove partition clock cache for a matrix
   * @param matrixId
   */
  public void removeMatrix(int matrixId) {
    matrixClockCacheMap.remove(matrixId);
  }

  /**
   * Clocks sync thread. The clocks are stored on ps, it synchronizes the clocks to the local at
   * regular intervals.
   */
  class Syncer extends Thread {
    private final MatrixTransportInterface matrixClient = PSAgentContext.get().getMatrixTransportClient();
    private final ParameterServerId[] serverIds = PSAgentContext.get().getLocationManager().getPsIds();
    private final ClockCache cache = PSAgentContext.get().getClockCache();

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      @SuppressWarnings("rawtypes")
      Map<ParameterServerId, Future> psIdToResultMap = new HashMap<>(serverIds.length);
      long startTsMs = 0;
      long useTimeMs = 0;
      int syncNum = 0;
      while (!stopped.get() && !Thread.interrupted()) {
        startTsMs = System.currentTimeMillis();
        // Send request to every ps
        for (int i = 0; i < serverIds.length; i++) {
          try {
            psIdToResultMap.put(serverIds[i], matrixClient.getClocks(serverIds[i]));
          } catch (Exception e) {
            LOG.error("get clocks failed from server " + serverIds[i] + " failed, ", e);
          }
        }

        // Wait the responses
        try {
          for(Entry<ParameterServerId, Future> resultEntry : psIdToResultMap.entrySet()) {
            GetClocksResponse response = (GetClocksResponse) resultEntry.getValue().get();
            if(response.getResponseType() == ResponseType.SUCCESS) {
              Map<PartitionKey, Integer> clocks = response.getClocks();
              for(Entry<PartitionKey, Integer> entry:clocks.entrySet()) {
                // Update clock cache
                cache.update(entry.getKey().getMatrixId(), entry.getKey(), entry.getValue());
              }

              if(LOG.isDebugEnabled()) {
                //if(syncNum % 1024 == 0) {
                for(Entry<PartitionKey, Integer> entry:clocks.entrySet()) {
                  LOG.debug("partition " + entry.getKey() + " update clock to " + entry.getValue());
                }
                //}
              }
            } else {
              LOG.error("Get clock from ps " + resultEntry.getKey()
                + ", failed. Detail log is " + response.getResponseType()
                + ":" + response.getDetail());
              PSAgentContext.get().getLocationManager().getPsLocation(resultEntry.getKey(), true);
            }
          }
          psIdToResultMap.clear();

          useTimeMs = System.currentTimeMillis() - startTsMs;
          if (useTimeMs < syncTimeIntervalMS) {
            Thread.sleep(syncTimeIntervalMS - useTimeMs);
          }

          syncNum++;
        } catch(InterruptedException ie) {
          LOG.info("sync thread is interrupted");
        } catch (Exception e) {
          LOG.error("get clocks failed, ", e);
        }
      }
    }
  }

  /**
   * Add matrix clock cache
   * 
   * @param matrixId matrix id
   * @param parts matrix partitons
   */
  public void addMatrix(int matrixId, List<PartitionKey> parts) {
    if (!matrixClockCacheMap.containsKey(matrixId)) {
      matrixClockCacheMap.putIfAbsent(matrixId, new MatrixClockCache(matrixId, parts));
    }
  }

  /**
   * Update matrix partition clock
   *  
   * @param matrixId matrix id
   * @param partKey partition key
   * @param clock clock value
   */
  public void update(int matrixId, PartitionKey partKey, int clock) {
    LOG.debug("partition " + partKey + " clock update to " + clock);
    MatrixClockCache matrixClockCache = matrixClockCacheMap.get(matrixId);
    if (matrixClockCache == null) {
      matrixClockCacheMap.putIfAbsent(matrixId, new MatrixClockCache(matrixId));
      matrixClockCache = matrixClockCacheMap.get(matrixId);
    }
    if(matrixClockCache.getClock(partKey) < clock) {
      matrixClockCache.update(partKey, clock);
    }
  }

  /**
   * Get a matrix partition clock
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @return int clock
   */
  public int getClock(int matrixId, PartitionKey partKey) {
    MatrixClockCache matrixClockCache = matrixClockCacheMap.get(matrixId);
    if (matrixClockCache == null) {
      return 0;
    }
    return matrixClockCache.getClock(partKey);
  }

  /**
   * Get a matrix row clock
   * 
   * @param matrixId matrix id
   * @param rowIndex row index
   * @return int clock
   */
  public int getClock(int matrixId, int rowIndex) {
    MatrixClockCache matrixClockCache = matrixClockCacheMap.get(matrixId);
    if (matrixClockCache == null) {
      return 0;
    }
    return matrixClockCache.getClock(rowIndex);
  }

  /**
   * Get a matrix clock cache
   * @param matrixId
   * @return MatrixClockCache
   */
  public MatrixClockCache getMatrixClockCache(int matrixId) {
    return matrixClockCacheMap.get(matrixId);
  }
}
