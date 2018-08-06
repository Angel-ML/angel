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

package com.tencent.angel.psagent.matrix.cache;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Matrix cache manager. It managers a matrix cache {@link MatrixCache} for every matrix.
 */
public class MatricesCache {
  /**matrix id to matrix cache map*/
  private final ConcurrentHashMap<Integer, MatrixCache> matricesCacheMap;
  
  /**matrices cache sync thread*/
  private Syncer syncer;
  
  /**cache sync time interval in milliseconds*/
  private int syncTimeIntervalMS;
  
  /**stop syncer or not*/
  private final AtomicBoolean stopped;
  
  /**cache sync policy*/
  private SyncPolicy syncPolicy;

  /**
   * Create a new MatricesCache.
   */
  public MatricesCache() {
    matricesCacheMap = new ConcurrentHashMap<Integer, MatrixCache>();
    stopped = new AtomicBoolean(false);
  }

  /**
   * Remove cache data for a matrix
   * @param matrixId
   */
  public void remove(int matrixId) {
    matricesCacheMap.remove(matrixId);
  }

  /**
   * Matrices cache sync thread.
   */
  class Syncer extends Thread {
    @Override
    public void run() {
      while (!stopped.get() && !Thread.interrupted()) {
        syncPolicy.sync(PSAgentContext.get().getMatricesCache());
        try {
          Thread.sleep(syncTimeIntervalMS);
        } catch (InterruptedException e) {

        }
      }
    }
  }

  /**
   * Startup cache sync thread.
   * 
   * @throws InstantiationException create a instance of sync policy class failed
   * @throws IllegalAccessException create a instance of sync policy class failed
   * @throws ClassNotFoundException sync policy class is not found
   */
  public void start() throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    syncTimeIntervalMS =
        PSAgentContext
            .get()
            .getConf()
            .getInt(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS,
                AngelConf.DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS);
    String syncPolicyClass =
        PSAgentContext
            .get()
            .getConf()
            .get(AngelConf.ANGEL_PSAGENT_CACHE_SYNC_POLICY_CLASS,
                AngelConf.DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_POLICY_CLASS);
    syncPolicy = (SyncPolicy) Class.forName(syncPolicyClass).newInstance();

    syncer = new Syncer();
    syncer.setName("matrixcache-syncer");
    syncer.start();
  }

  /**
   * Stop matrices cache sync thread.
   */
  public void stop() {
    if(!stopped.getAndSet(true)){
      if (syncer != null) {
        syncer.interrupt();
        syncer = null;
      }

      matricesCacheMap.clear();
    }
  }

  /**
   * Get a row split from cache
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @param rowIndex row index
   * @return row split
   */
  public ServerRow getRowSplit(int matrixId, PartitionKey partKey, int rowIndex) {
    MatrixCache matrixCache = matricesCacheMap.get(matrixId);
    if (matrixCache == null) {
      return null;
    }
    return matrixCache.getRowSplit(partKey, rowIndex);
  }

  /**
   * Get a matrix partition from cache
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @return matrix partition
   */
  public ServerPartition getPartition(int matrixId, PartitionKey partKey) {
    MatrixCache matrixCache = matricesCacheMap.get(matrixId);
    if (matrixCache == null) {
      return null;
    }
    return matrixCache.getPartition(partKey);
  }

  /**
   * Get a batch of row splits that belong to a matrix partition
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @param rowIndexes row indexes
   * @return a batch of row splits
   */
  public List<ServerRow> getRowsSplit(int matrixId, PartitionKey partKey, List<Integer> rowIndexes) {
    MatrixCache matrixCache = matricesCacheMap.get(matrixId);
    if (matrixCache == null) {
      return null;
    }
    return matrixCache.getRowsSplit(partKey, rowIndexes);
  }

  public ConcurrentHashMap<Integer, MatrixCache> getMatricesCacheMap() {
    return matricesCacheMap;
  }
  
  /**
   * Get a matrix cache
   * 
   * @param matrixId matrix id
   * @return  the cache of the matrix
   */
  public MatrixCache getMatrixCache(int matrixId) {
    return matricesCacheMap.get(matrixId);
  }

  /**
   * Update a matrix partition in the cache
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @param part matrix partition
   */
  public void update(int matrixId, PartitionKey partKey, ServerPartition part) {
    MatrixCache matrixCache = matricesCacheMap.get(matrixId);
    if (matrixCache == null) {
      matricesCacheMap.putIfAbsent(matrixId, new MatrixCache(matrixId));
      matrixCache = matricesCacheMap.get(matrixId);
    }

    matrixCache.update(partKey, part);
  }

  /**
   * Update a row split in the cache
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @param rowSplit row split
   */
  public void update(int matrixId, PartitionKey partKey, ServerRow rowSplit) {
    MatrixCache matrixCache = matricesCacheMap.get(matrixId);
    if (matrixCache == null) {
      matricesCacheMap.putIfAbsent(matrixId, new MatrixCache(matrixId));
      matrixCache = matricesCacheMap.get(matrixId);
    }

    matrixCache.update(partKey, rowSplit);
  }

  /**
   * Update a batch row splits in the cache
   * 
   * @param matrixId matrix id
   * @param partKey partition key
   * @param rowsSplit a batch row splits
   */
  public void update(int matrixId, PartitionKey partKey, List<ServerRow> rowsSplit) {
    MatrixCache matrixCache = matricesCacheMap.get(matrixId);
    if (matrixCache == null) {
      matricesCacheMap.putIfAbsent(matrixId, new MatrixCache(matrixId));
      matrixCache = matricesCacheMap.get(matrixId);
    }

    matrixCache.update(partKey, rowsSplit);
  }
  
  /**
   * Clean whole cache.
   */
  public void clear() {
    matricesCacheMap.clear();
  }
  
  /**
   * Clean a matrix from cache
   * 
   * @param matrixId matrix id
   */
  public void clear(int matrixId) {
    matricesCacheMap.remove(matrixId);
  }
  
  /**
   * Clean a matrix partition from cache
   * 
   * @param matrixId matrix id
   * @param partitionKey partition key
   */
  public void clear(int matrixId, PartitionKey partitionKey) {
    MatrixCache cache = matricesCacheMap.get(matrixId);
    if(cache != null) {
      cache.clear(partitionKey);
    }
  }
}
