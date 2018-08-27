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


package com.tencent.angel.psagent.matrix.transport.adapter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Sub-request results cache.
 */
public class PartitionResponseCache<T> {
  private static final Log LOG = LogFactory.getLog(PartitionResponseCache.class);

  /**
   * sub-requests number
   */
  protected final int totalRequestNum;

  /**
   * Sub responses
   */
  private final List<T> subResponses;

  /**
   * Sub responses read position
   */
  private int readPos;

  /**
   * Sub responses write position
   */
  private int writePos;

  protected final Lock lock;

  /**
   * Create a new PartitionResponseCache.
   *
   * @param totalRequestNum sub-requests number
   */
  public PartitionResponseCache(int totalRequestNum) {
    this(totalRequestNum, totalRequestNum);
  }

  /**
   * Create a new PartitionResponseCache.
   *
   * @param totalRequestNum sub-requests number
   */
  public PartitionResponseCache(int totalRequestNum, int capacity) {
    this.totalRequestNum = totalRequestNum;
    subResponses = new ArrayList<T>(capacity);
    readPos = 0;
    writePos = 0;
    lock = new ReentrantLock();
  }

  /**
   * Receive a sub-response
   *
   * @param subResponse
   */
  public void addSubResponse(T subResponse) {
    try {
      lock.lock();
      subResponses.add(subResponse);
      writePos++;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Check if all results are received.
   *
   * @return boolean true means all results are received
   */
  public boolean isReceivedOver() {
    try {
      lock.lock();
      return subResponses.size() >= totalRequestNum;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get the sub-requests number.
   *
   * @return int the sub-requests number
   */
  public int getTotalRequestNum() {
    return totalRequestNum;
  }

  /**
   * Get the received results number.
   *
   * @return int the received results number
   */
  public int getReceivedResponseNum() {
    try {
      lock.lock();
      return subResponses.size();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Can merge sub-response now
   *
   * @return true means can merge now
   */
  public boolean canMerge() {
    return isReceivedOver();
  }

  /**
   * Get sub responses, it is lock free, you should call isReceivedOver before call it
   *
   * @return sub responses
   */
  public List<T> getSubResponses() {
    return subResponses;
  }

  /**
   * Get next sub response
   *
   * @return next sub response, null means it not ready or get all sub responses already
   */
  public T readNextSubResponse() {
    while (true) {
      try {
        lock.lock();
        if (writePos > readPos) {
          T ret = subResponses.get(readPos);
          readPos++;
          return ret;
        } else if ((readPos == writePos) && (writePos == totalRequestNum)) {
          return null;
        }
      } finally {
        lock.unlock();
      }

      try {
        Thread.sleep(5);
      } catch (InterruptedException e) {

      }
    }
  }

  /**
   * Reset read position
   */
  public void reset() {
    try {
      lock.lock();
      readPos = 0;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Get the request progress.
   *
   * @return double the request progress
   */
  public double getProgress() {
    return ((double) getReceivedResponseNum()) / getTotalRequestNum();
  }

  @Override public String toString() {
    return "PartitionResponseCache{" + "totalRequestNum=" + totalRequestNum + ", subResponses="
      + subResponses.size() + ", readPos=" + readPos + ", writePos=" + writePos + '}';
  }
}
