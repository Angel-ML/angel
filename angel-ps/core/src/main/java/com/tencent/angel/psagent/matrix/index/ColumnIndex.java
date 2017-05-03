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

package com.tencent.angel.psagent.matrix.index;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ColumnIndex {
  private IntOpenHashSet indexSet;
  private int[] index;
  private final Lock readLock;
  private final Lock writeLock;
  private boolean indexUpdated;

  public ColumnIndex() {
    indexSet = new IntOpenHashSet();
    index = null;
    indexUpdated = false;
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    readLock = lock.readLock();
    writeLock = lock.writeLock();
  }

  public void addIndexes(int[] indexArray) {
    try {
      writeLock.lock();
      for (int i = 0; i < indexArray.length; i++) {
        indexSet.add(indexArray[i]);
      }

      indexUpdated = true;
    } finally {
      writeLock.unlock();
    }
  }

  public void addIndexes(IntOpenHashSet indexSet) {
    try {
      writeLock.lock();
      this.indexSet.addAll(indexSet);
      indexUpdated = true;
    } finally {
      writeLock.unlock();
    }
  }

  public void merge(ColumnIndex oldIndex) {
    if (this == oldIndex) {
      return;
    }

    try {
      writeLock.lock();
      try {
        oldIndex.readLock.lock();
        indexSet.addAll(oldIndex.indexSet);
        indexUpdated = true;
      } finally {
        oldIndex.readLock.unlock();
      }
    } finally {
      writeLock.unlock();
    }
  }

  public int[] getIndex() {
    try {
      readLock.lock();
      if (indexUpdated || index == null) {
        index = indexSet.toArray(new int[0]);
        indexUpdated = false;
      }
      return index;
    } finally {
      readLock.unlock();
    }
  }
}
