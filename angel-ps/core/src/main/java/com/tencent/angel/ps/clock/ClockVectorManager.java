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


package com.tencent.angel.ps.clock;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import com.tencent.angel.ps.PSContext;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Matrix partition clock vector manager
 */
public class ClockVectorManager {
  private static final Log LOG = LogFactory.getLog(ClockVectorManager.class);
  private final PSContext context;
  /**
   * Matrix id to matrix clock vector map
   */
  private final ConcurrentHashMap<Integer, MatrixClockVector> matrixIdToClockVecMap;

  /**
   * Total task number in application
   */
  private final int taskNum;

  /**
   * Partition key to clock value map
   */
  private final ConcurrentHashMap<PartitionKey, Integer> partKeyToClockMap;

  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile Thread adjustThread;

  /**
   * Create a ClockVectorManager
   *
   * @param taskNum total task number
   */
  public ClockVectorManager(int taskNum, PSContext context) {
    this.matrixIdToClockVecMap = new ConcurrentHashMap<>();
    this.taskNum = taskNum;
    this.context = context;
    partKeyToClockMap = new ConcurrentHashMap<>();
  }

  public void init() {

  }

  public void start() {
    if (context.getPSAttemptId().getIndex() > 0 && context.getPartReplication() == 1) {
      adjustThread = new Thread(() -> {
        while (!stopped.get() && !Thread.interrupted()) {
          try {
            Thread.sleep(30000);
            adjustClocks(context.getMaster().getTaskMatrixClocks());
          } catch (Throwable e) {
            if (!stopped.get()) {
              LOG.error("Get clock vector from master failed ", e);
            }
          }
        }
      });
      adjustThread.setName("Adjust-Clock-Thread");
      adjustThread.start();
    }
  }

  public void stop() {
    if (!stopped.getAndSet(true)) {
      if (adjustThread != null) {
        adjustThread.interrupt();
        adjustThread = null;
      }
    }
  }

  /**
   * Generate clock vectors for a batch of matrices
   *
   * @param matrixMetas matrices meta
   */
  public void addMatrices(List<MatrixMeta> matrixMetas) {
    int size = matrixMetas.size();
    for (int i = 0; i < size; i++) {
      addMatrix(matrixMetas.get(i));
    }
  }

  /**
   * Generate the clock vector for a matrix
   *
   * @param matrixMeta matrix meta
   */
  public void addMatrix(MatrixMeta matrixMeta) {
    if (!matrixIdToClockVecMap.containsKey(matrixMeta.getId())) {
      matrixIdToClockVecMap
        .putIfAbsent(matrixMeta.getId(), new MatrixClockVector(taskNum, matrixMeta));
      for (PartitionMeta partMeta : matrixMeta.getPartitionMetas().values()) {
        partKeyToClockMap.put(partMeta.getPartitionKey(), 0);
      }
    }
  }

  /**
   * Remove matrices
   *
   * @param matrixIds matrices id
   */
  public void removeMatrices(List<Integer> matrixIds) {
    int size = matrixIds.size();
    for (int i = 0; i < size; i++) {
      removeMatrix(matrixIds.get(i));
    }
  }

  /**
   * Remove a matrix
   *
   * @param matrixId matrix id
   */
  public void removeMatrix(int matrixId) {
    matrixIdToClockVecMap.remove(matrixId);

    Iterator<Map.Entry<PartitionKey, Integer>> iter = partKeyToClockMap.entrySet().iterator();
    while (iter.hasNext()) {
      if (iter.next().getKey().getMatrixId() == matrixId) {
        iter.remove();
      }
    }
  }

  /**
   * Update clock value for a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @param taskId   task id
   * @param clock    clock value
   */
  public void updateClock(int matrixId, int partId, int taskId, int clock) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("update clock vector for a non-exist matrix " + matrixId);
      return;
    }
    matrixClockVector.updateClock(partId, taskId, clock);
  }

  /**
   * Update clock value for a matrix
   *
   * @param matrixId matrix id
   * @param taskId   task id
   * @param clock    clock value
   */
  public void updateClock(int matrixId, int taskId, int clock) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("update clock vector for a non-exist matrix " + matrixId);
      return;
    }
    matrixClockVector.updateClock(taskId, clock);
  }

  /**
   * Get clock value of a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return clock value
   */
  public int getPartClock(int matrixId, int partId) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("get clock vector for a non-exist matrix " + matrixId);
      return -1;
    }
    return matrixClockVector.getPartClock(partId);
  }

  /**
   * Get clock vector of a matrix partition
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @return clock vector
   */
  public Int2IntOpenHashMap getClockVec(int matrixId, int partId) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("get clock vector for a non-exist matrix " + matrixId);
      return new Int2IntOpenHashMap();
    }
    return matrixClockVector.getClockVec(partId);
  }

  /**
   * Get partition clocks for a matrix
   *
   * @param matrixId matrix id
   * @return partition clocks
   */
  public Int2IntOpenHashMap getPartClocks(int matrixId) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("get clock vector for a non-exist matrix " + matrixId);
      return new Int2IntOpenHashMap();
    }
    return matrixClockVector.getPartClocks();
  }

  /**
   * Get all matrices partition clocks
   *
   * @return all matrices partition clocks
   */
  public Map<PartitionKey, Integer> getPartClocksFromCache() {
    for (PartitionKey partKey : partKeyToClockMap.keySet()) {
      partKeyToClockMap.put(partKey, getPartClock(partKey.getMatrixId(), partKey.getPartitionId()));
    }

    return partKeyToClockMap;
  }

  /**
   * Get matrix clock
   *
   * @param matrixId matrix clock
   * @return matrix clock
   */
  public int getMatrixClock(int matrixId) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("get clock vector for a non-exist matrix " + matrixId);
      return -1;
    }
    return matrixClockVector.getMinClock();
  }

  /**
   * Adjust clock values
   *
   * @param taskToMatrixClocks taskId->(matrixId->clock) map
   */
  public void adjustClocks(Int2ObjectOpenHashMap<Int2IntOpenHashMap> taskToMatrixClocks) {
    ObjectIterator<Int2ObjectMap.Entry<Int2IntOpenHashMap>> taskIter =
      taskToMatrixClocks.int2ObjectEntrySet().fastIterator();
    Int2ObjectMap.Entry<Int2IntOpenHashMap> taskEntry = null;
    int taskId = 0;
    Int2IntOpenHashMap matrixIdToClockMap = null;
    ObjectIterator<Int2IntMap.Entry> matrixIter = null;
    Int2IntMap.Entry matrixEntry = null;

    while (taskIter.hasNext()) {
      taskEntry = taskIter.next();
      taskId = taskEntry.getIntKey();
      matrixIdToClockMap = taskEntry.getValue();
      matrixIter = matrixIdToClockMap.int2IntEntrySet().fastIterator();
      while (matrixIter.hasNext()) {
        matrixEntry = matrixIter.next();
        updateClock(matrixEntry.getIntKey(), taskId, matrixEntry.getIntValue());
      }
    }
  }

  /**
   * Set partition clock vector
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @param clockVec clock vector
   */
  public void setClockVec(int matrixId, int partId, Int2IntOpenHashMap clockVec) {
    MatrixClockVector matrixClockVector = matrixIdToClockVecMap.get(matrixId);
    if (matrixClockVector == null) {
      LOG.warn("update clock vector for a non-exist matrix " + matrixId);
      return;
    }
    matrixClockVector.setClockVec(partId, clockVec);
  }
}
