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

import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.PartitionMeta;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Matrix clock vector
 */
public class MatrixClockVector {
  /**
   * Partition id to partition clock vector map
   */
  private final ConcurrentHashMap<Integer, PartClockVector> partIdToClockVecMap;

  /**
   * Total task number
   */
  private final int taskNum;

  /**
   * Create a MatrixClockVector
   *
   * @param taskNum    total task number
   * @param matrixMeta matrix meta
   */
  public MatrixClockVector(int taskNum, MatrixMeta matrixMeta) {
    this.taskNum = taskNum;
    this.partIdToClockVecMap = new ConcurrentHashMap<>(matrixMeta.getPartitionMetas().size());
    initPartClockVectors(matrixMeta);
  }

  /**
   * Init partitions clock vector for matrix
   *
   * @param matrixMeta matrix meta
   */
  private void initPartClockVectors(MatrixMeta matrixMeta) {
    Map<Integer, PartitionMeta> partIdToMetaMap = matrixMeta.getPartitionMetas();
    for (Map.Entry<Integer, PartitionMeta> entry : partIdToMetaMap.entrySet()) {
      partIdToClockVecMap.put(entry.getKey(), new PartClockVector(taskNum));
    }
  }

  /**
   * Update task clock for a partition
   *
   * @param partId partition id
   * @param taskId task id
   * @param clock  clock value
   */
  public void updateClock(int partId, int taskId, int clock) {
    partIdToClockVecMap.get(partId).updateClock(taskId, clock);
  }

  /**
   * Get partition clock value
   *
   * @param partId partition id
   * @return partition clock value
   */
  public int getPartClock(int partId) {
    return partIdToClockVecMap.get(partId).getMinClock();
  }

  /**
   * Get partition clock vector
   *
   * @param partId partition id
   * @return clock vector
   */
  public Int2IntOpenHashMap getClockVec(int partId) {
    return partIdToClockVecMap.get(partId).getClockVec();
  }

  /**
   * Get the minimal clock value for all partitions
   *
   * @return the minimal clock value for all partitions
   */
  public int getMinClock() {
    int minClock = Integer.MAX_VALUE;
    for (PartClockVector partClockVector : partIdToClockVecMap.values()) {
      if (partClockVector.getMinClock() < minClock) {
        minClock = partClockVector.getMinClock();
      }
    }
    return minClock;
  }

  /**
   * Get partition id to partition clock map
   *
   * @return partition id to partition clock map
   */
  public Int2IntOpenHashMap getPartClocks() {
    Int2IntOpenHashMap partClocks = new Int2IntOpenHashMap(partIdToClockVecMap.size());
    for (Map.Entry<Integer, PartClockVector> entry : partIdToClockVecMap.entrySet()) {
      partClocks.put(entry.getKey().intValue(), entry.getValue().getMinClock());
    }
    return partClocks;
  }

  /**
   * Update task clock for all partitions
   *
   * @param taskId task id
   * @param clock  clock value
   */
  public void updateClock(int taskId, int clock) {
    for (int partId : partIdToClockVecMap.keySet()) {
      updateClock(partId, taskId, clock);
    }
  }

  /**
   * Set partition clock vector
   *
   * @param partId   partition id
   * @param clockVec clock vector
   */
  public void setClockVec(int partId, Int2IntOpenHashMap clockVec) {
    partIdToClockVecMap.get(partId).setClockVec(clockVec);
  }
}
