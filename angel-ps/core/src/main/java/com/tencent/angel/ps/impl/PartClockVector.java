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

package com.tencent.angel.ps.impl;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Matrix partition clock vector
 */
public class PartClockVector {
  private static final Log LOG = LogFactory.getLog(PartClockVector.class);
  /**
   * Min clock value for this partition
   */
  private int minClock;

  /**
   * Task id to clock value map
   */
  private final Int2IntOpenHashMap taskIndexToClockMap;
  private final ReadWriteLock lock;

  /**
   * Total task number
   */
  private final int taskNum;

  /**
   * Create a PartClockVector
   * @param taskNum total task number
   */
  public PartClockVector(int taskNum) {
    this.taskNum = taskNum;
    minClock = 0;
    taskIndexToClockMap = new Int2IntOpenHashMap(taskNum);
    for(int i = 0; i < taskNum; i++) {
      taskIndexToClockMap.put(i, 0);
    }
    lock = new ReentrantReadWriteLock();
  }

  /**
   * Update a task clock value
   * @param taskIndex task index
   * @param clock clock value
   */
  public void updateClock(int taskIndex, int clock) {
    try {
      lock.writeLock().lock();
      if (!taskIndexToClockMap.containsKey(taskIndex)) {
        taskIndexToClockMap.put(taskIndex, clock);
      } else {
        int oldClock = taskIndexToClockMap.get(taskIndex);
        if (oldClock < clock) {
          taskIndexToClockMap.put(taskIndex, clock);
        }
      }

      if (minClock < clock) {
        refreshMinClock();
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void refreshMinClock() {
    if (taskIndexToClockMap.size() < taskNum) {
      minClock = 0;
      return;
    }

    int min = Integer.MAX_VALUE;
    for (Int2IntMap.Entry entry : taskIndexToClockMap.int2IntEntrySet()) {
      if (entry.getIntValue() < min) {
        min = entry.getIntValue();
      }
    }

    if (minClock < min) {
      minClock = min;
    }
  }

  /**
   * Gets represented clock.
   *
   * @return the clock
   */
  public int getClock() {
    try {
      lock.readLock().lock();
      return minClock;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get min clock
   * @return min clock
   */
  public int getMinClock() {
    return minClock;
  }

  /**
   * Get clock vector
   * @return clock vector
   */
  public Int2IntOpenHashMap getClockVec() {
    try {
      lock.readLock().lock();
      return taskIndexToClockMap.clone();
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Set clock vector
   * @param clockVec clock vector
   */
  public void setClockVec(Int2IntOpenHashMap clockVec) {
    try {
      lock.writeLock().lock();
      ObjectIterator<Int2IntMap.Entry> iter = clockVec.int2IntEntrySet().fastIterator();
      Int2IntMap.Entry item;
      while(iter.hasNext()) {
        item = iter.next();
        if(!taskIndexToClockMap.containsKey(item.getIntKey())
          || (taskIndexToClockMap.containsKey(item.getIntKey())
          && taskIndexToClockMap.get(item.getIntKey()) < item.getIntValue())) {
          taskIndexToClockMap.put(item.getIntKey(), item.getIntValue());
        }
      }
      refreshMinClock();
    } finally {
      lock.writeLock().unlock();
    }
  }
}
