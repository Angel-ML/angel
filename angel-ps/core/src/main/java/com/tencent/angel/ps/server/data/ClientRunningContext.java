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


package com.tencent.angel.ps.server.data;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ClientRunningContext {
  private static final Log LOG = LogFactory.getLog(ClientRunningContext.class);
  private final AtomicInteger totalRPCCounter = new AtomicInteger(0);
  private final AtomicInteger totalRunningRPCCounter = new AtomicInteger(0);
  private final AtomicInteger inflightingRPCCounter = new AtomicInteger(0);
  private volatile long lastUpdateTs;
  private final SlidingWindow window = new SlidingWindow();

  /**
   * Release token for this client
   *
   * @param tokenNum
   */
  public void releaseToken(int tokenNum) {
    inflightingRPCCounter.addAndGet(-tokenNum);
    lastUpdateTs = System.currentTimeMillis();
  }

  /**
   * Allocate token for this client
   *
   * @param tokenNum token number
   */
  public void allocateToken(int tokenNum) {
    inflightingRPCCounter.addAndGet(tokenNum);
    lastUpdateTs = System.currentTimeMillis();
  }

  public void printToken() {
    LOG.info("+++++++++++++++++++++Client running context start+++++++++++++++++++++");
    LOG.info("totalRunningRPCCounter = " + totalRunningRPCCounter.get());
    LOG.info("infligtingRPCCounter = " + inflightingRPCCounter.get());
    LOG.info("totalRPCCounter = " + totalRPCCounter.get());
    LOG.info("lastUpdateTs = " + lastUpdateTs);
    LOG.info("+++++++++++++++++++++Client running context end  +++++++++++++++++++++");
  }

  public class SlidingWindow {

  }

  /**
   * Receive a request and start to handle it
   *
   * @param seqId request seqId
   */
  public void before(int seqId) {
    totalRPCCounter.incrementAndGet();
    totalRunningRPCCounter.incrementAndGet();
  }

  /**
   * Send the response for the request
   *
   * @param seqId request id
   */
  public void after(int seqId) {
    totalRunningRPCCounter.decrementAndGet();
  }

  /**
   * Get last token update time
   *
   * @return last token update time
   */
  public long getLastUpdateTs() {
    return lastUpdateTs;
  }

  /**
   * Get infighting rpc number from this client
   *
   * @return infighting rpc number from this client
   */
  public int getInflightingRPCCounter() {
    return inflightingRPCCounter.get();
  }
}
