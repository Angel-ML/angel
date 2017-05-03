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

package com.tencent.angel.utils;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class VectorClock {

  private ConcurrentHashMap<String, Integer> clocks;

  private int minClock;

  private ReentrantReadWriteLock lock;

  public VectorClock() {
    clocks = new ConcurrentHashMap<String, Integer>();
    minClock = 0;
    lock = new ReentrantReadWriteLock();
  }

  public VectorClock(String[] ids) {
    int length = ids.length;
    int clock = 0;
    for (int i = 0; i < length; i++)
      clocks.put(ids[i], clock);
    minClock = 0;
  }

  public int getMinClock() {
    try {
      lock.readLock().lock();
      return minClock;
    } finally {
      lock.readLock().unlock();
    }
  }

  public void addClock(String workerId) {
    try {
      lock.writeLock().lock();
      clocks.putIfAbsent(workerId, 0);
      minClock = 0;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public int getClock(String workerId) {
    try {
      lock.readLock().lock();
      return clocks.get(workerId);
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Increment client ``id`` clock.
   */
  public int tick(String id) {
    try {
      lock.writeLock().lock();
      int value = clocks.get(id);
      clocks.put(id, value + 1);
      boolean found = false;
      if (value == minClock) {
        Iterator<Integer> iter = clocks.values().iterator();
        while (iter.hasNext()) {
          if (iter.next() == minClock) {
            found = true;
            break;
          }
        }
        if (!found) {
          minClock += 1;
        }
      }
      return minClock;
    } finally {
      lock.writeLock().unlock();
    }
  }

  public String formatString() {
    StringBuilder str = new StringBuilder();
    try {
      lock.readLock().lock();
      Iterator<String> iter = clocks.keySet().iterator();
      while (iter.hasNext()) {
        String workerId = iter.next();
        int clock = clocks.get(workerId);
        str.append(workerId + ":" + clock);
        if (!iter.hasNext()) {
          str.append(";");
        }
      }
      return str.toString();
    } finally {
      lock.readLock().unlock();
    }
  }
}
