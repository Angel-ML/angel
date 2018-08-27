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


package com.tencent.angel.ml.matrix;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.ParameterServerId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Matrix partition meta
 */
public class PartitionMeta {

  /**
   * Partition base information
   */
  PartitionKey partitionKey;

  /**
   * Where is this partition stored. A partition may be stored in more then one ParameterServers,
   * this first ParameterServer is the Master for this partition, the Others are Slaves. Client
   * can only get/put this partition with the Master ParameterServer
   */
  private List<ParameterServerId> storedPs;
  private final ReadWriteLock lock;

  /**
   * Create a PartitionMeta
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @param startRow start row index
   * @param endRow   end row index
   * @param startCol start column index
   * @param endCol   end column index
   */
  public PartitionMeta(int matrixId, int partId, int startRow, int endRow, long startCol,
    long endCol) {
    this(matrixId, partId, startRow, endRow, startCol, endCol, new ArrayList<>());
  }

  /**
   * Create a PartitionMeta
   *
   * @param partitionKey partition basic information
   */
  public PartitionMeta(PartitionKey partitionKey) {
    this(partitionKey, new ArrayList<>());
  }

  /**
   * Create a PartitionMeta
   *
   * @param partitionKey partition basic information
   * @param storedPs     the ParameterServers this partition is stored
   */
  public PartitionMeta(PartitionKey partitionKey, List<ParameterServerId> storedPs) {
    this.partitionKey = partitionKey;
    this.storedPs = storedPs;
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Create a PartitionMeta
   *
   * @param matrixId matrix id
   * @param partId   partition id
   * @param startRow start row index
   * @param endRow   end row index
   * @param startCol start column index
   * @param endCol   end column index
   * @param storedPs the ParameterServers this partition is stored
   */
  public PartitionMeta(int matrixId, int partId, int startRow, int endRow, long startCol,
    long endCol, List<ParameterServerId> storedPs) {
    this.partitionKey = new PartitionKey(partId, matrixId, startRow, startCol, endRow, endCol);
    this.storedPs = storedPs;
    this.lock = new ReentrantReadWriteLock();
  }

  /**
   * Get the Master ParameterServer for this partition
   *
   * @return the Master ParameterServer for this partition
   */
  public ParameterServerId getMasterPs() {
    try {
      lock.readLock().lock();
      if (storedPs.isEmpty()) {
        return null;
      } else {
        return storedPs.get(0);
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Remove a ParameterServer which this partition is stored on
   *
   * @param psId ParameterServer id
   */
  public void removePs(ParameterServerId psId) {
    try {
      lock.writeLock().lock();
      int size = storedPs.size();
      for (int i = 0; i < size; i++) {
        if (storedPs.get(i).equals(psId)) {
          storedPs.remove(i);
          return;
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Make a ParameterServer to the Master ParameterServer
   *
   * @param psId ParameterServer id
   */
  public void makePsToMaster(ParameterServerId psId) {
    try {
      lock.writeLock().lock();
      int size = storedPs.size();
      for (int i = 0; i < size; i++) {
        if (storedPs.get(i).equals(psId)) {
          if (i == 0) {
            return;
          } else {
            storedPs.set(i, storedPs.get(0));
            storedPs.set(0, psId);
            return;
          }
        }
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get the partition id
   *
   * @return the partition id
   */
  public int getPartId() {
    return partitionKey.getPartitionId();
  }

  /**
   * Get the start row index
   *
   * @return the start row index
   */
  public int getStartRow() {
    return partitionKey.getStartRow();
  }

  /**
   * Get the end row index
   *
   * @return the end row index
   */
  public int getEndRow() {
    return partitionKey.getEndRow();
  }

  /**
   * Get the start column index
   *
   * @return the start column index
   */
  public long getStartCol() {
    return partitionKey.getStartCol();
  }

  /**
   * Get the end column index
   *
   * @return the end column index
   */
  public long getEndCol() {
    return partitionKey.getEndCol();
  }

  /**
   * Get the stored ParameterServers
   *
   * @return the stored ParameterServers
   */
  public List<ParameterServerId> getPss() {
    try {
      lock.readLock().lock();
      return new ArrayList<>(storedPs);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Add a PS which a partitin replication stored in
   *
   * @param psId ParameterServer id
   */
  public void addReplicationPS(ParameterServerId psId) {
    try {
      lock.writeLock().lock();

      int size = storedPs.size();
      boolean found = false;
      for (int i = 0; i < size; i++) {
        if (storedPs.get(i).equals(psId)) {
          found = true;
          break;
        }
      }

      if (!found) {
        storedPs.add(psId);
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get partition base information
   *
   * @return partition base information
   */
  public PartitionKey getPartitionKey() {
    return partitionKey;
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("partition meta:").append(partitionKey).append("\n");
    sb.append("stored pss:");
    int size = storedPs.size();
    for (int i = 0; i < size; i++) {
      sb.append(storedPs.get(i)).append(",");
    }
    sb.append("\n");

    return sb.toString();
  }

  /**
   * Set stored pss
   *
   * @param psIds ps ids
   */
  public void setPss(List<ParameterServerId> psIds) {
    try {
      lock.writeLock().lock();
      storedPs = psIds;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Is partition contain the row
   *
   * @param rowIndex row index
   * @return true means contain this row
   */
  public boolean contain(int rowIndex) {
    return rowIndex >= partitionKey.getStartRow() && rowIndex < partitionKey.getEndRow();
  }
}
