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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sub-request results cache.
 */
public class PartitionResponseCache {
  private static final Log LOG = LogFactory.getLog(PartitionResponseCache.class);

  /** sub-requests number */
  private final int totalRequestNum;

  /** received results number */
  private final AtomicInteger receivedResponseNum;

  /** the counter for the sub-requests which results are not received */
  private final CountDownLatch counter;

  /** the sub-requests are being merged */
  private final AtomicBoolean isMerging;

  /**
   * Create a new PartitionResponseCache.
   *
   * @param totalRequestNum sub-requests number
   */
  public PartitionResponseCache(int totalRequestNum) {
    this.totalRequestNum = totalRequestNum;
    receivedResponseNum = new AtomicInteger(0);
    counter = new CountDownLatch(totalRequestNum);
    isMerging = new AtomicBoolean(false);
  }

  /**
   * Update the received results number.
   * 
   * @param partId the partition index of the sub-request
   */
  public void updateReceivedResponse(int partId) {
    receivedResponseNum.incrementAndGet();
    counter.countDown();
    LOG.debug("receivedResponseNum=" + receivedResponseNum.get() + ", totalRequestNum="
        + totalRequestNum + ", counter=" + counter.getCount());
  }

  /**
   * Update the received results number.
   */
  public void updateReceivedResponse() {
    receivedResponseNum.incrementAndGet();
    counter.countDown();
    LOG.debug("receivedResponseNum=" + receivedResponseNum.get() + ", totalRequestNum="
        + totalRequestNum + ", counter=" + counter.getCount());
  }

  /**
   * Check if all results are received.
   * 
   * @return boolean true means all results are received
   */
  public boolean isReceivedOver() {
    return receivedResponseNum.get() >= totalRequestNum;
  }

  /**
   * Wait until all results are received.
   * 
   * @throws InterruptedException interrupted while waiting
   */
  public void waitForReceivedOver() throws InterruptedException {
    counter.await();
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
    return receivedResponseNum.get();
  }

  /**
   * Get the request progress.
   * 
   * @return double the request progress
   */
  public double getProgress() {
    return ((double) getReceivedResponseNum()) / getTotalRequestNum();
  }

  /**
   * Set the merging mark.
   * 
   * @param merging true means the sub-request results are being merged now
   */
  public void setIsMerging(boolean merging) {
    isMerging.set(merging);
  }

  /**
   * Get the merging mark.
   * 
   * @return boolean true means the sub-request results are being merged now
   */
  public boolean getIsMerging() {
    return isMerging.get();
  }

  @Override
  public String toString() {
    return "PartitionResponseCache [totalRequestNum=" + totalRequestNum + ", receivedResponseNum="
        + receivedResponseNum + ", counter=" + counter + "]";
  }
}
