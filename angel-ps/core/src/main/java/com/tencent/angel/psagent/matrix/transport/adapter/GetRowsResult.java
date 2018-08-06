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

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The result of get rows flow request.
 */
public class GetRowsResult {
  private static TVector wakeUpVector = new DenseDoubleVector(0);

  /** If all rows are fetched */
  private final AtomicBoolean isFetchOver;

  /** the queue of the received rows */
  private final LinkedBlockingQueue<TVector> newRowsQueue;

  /** the counter of rows that are fetched */
  private final AtomicInteger rowCounter;

  /**
   * Create a new GetRowsResult.
   */
  public GetRowsResult() {
    isFetchOver = new AtomicBoolean(false);
    newRowsQueue = new LinkedBlockingQueue<TVector>();
    rowCounter = new AtomicInteger(0);
  }

  /**
   * Put a row to queue.
   * 
   * @param row the row need to put to the queue
   * @throws InterruptedException interrupted while wait for queue space
   */
  public void put(TVector row) throws InterruptedException {
    newRowsQueue.put(row);
    rowCounter.incrementAndGet();
  }

  /**
   * Get a row from queue, if the queue is empty or all rows are consumed, just return null.
   * 
   * @return TVector the row get from the queue
   */
  public TVector poll() {
    TVector ret = newRowsQueue.poll();
    if (ret == wakeUpVector) {
      ret = null;
    }

    return ret;
  }

  /**
   * Get a row from queue, it will wait if the queue is empty. If the all rows are consumed, just
   * return null.
   * 
   * @throws InterruptedException interrupted while is blocked by queue.
   * @return TVector the row get from the queue
   */
  public TVector take() throws InterruptedException {
    if (isFetchOver.get() && newRowsQueue.peek() == null) {
      return null;
    }

    TVector row = newRowsQueue.take();
    if (isFetchOver.get() && row == wakeUpVector) {
      return null;
    } else {
      return row;
    }
  }

  /**
   * Get the number of rows received.
   * 
   * @return int the number of rows received
   */
  public int getRowsNumber() {
    return rowCounter.get();
  }

  /**
   * Set the mark that all rows are received.
   */
  public void fetchOver() {
    isFetchOver.set(true);
    try {
      newRowsQueue.put(wakeUpVector);
    } catch (InterruptedException e) {

    }
  }

  /**
   * Is all rows are received.
   * 
   * @return boolean true means all rows needed are received
   */
  public boolean isFetchOver() {
    return isFetchOver.get();
  }
}
