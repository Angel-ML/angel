package com.tencent.angel.ps.impl;

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
   * @param tokenNum
   */
  public void releaseToken(int tokenNum) {
    inflightingRPCCounter.addAndGet(-tokenNum);
    lastUpdateTs = System.currentTimeMillis();
  }

  /**
   * Allocate token for this client
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
   * @param seqId request seqId
   */
  public void before(int seqId) {
    totalRPCCounter.incrementAndGet();
    totalRunningRPCCounter.incrementAndGet();
  }

  /**
   * Send the response for the request
   * @param seqId request id
   */
  public void after(int seqId) {
    totalRunningRPCCounter.decrementAndGet();
  }

  /**
   * Get last token update time
   * @return last token update time
   */
  public long getLastUpdateTs() {
    return lastUpdateTs;
  }

  /**
   * Get infighting rpc number from this client
   * @return infighting rpc number from this client
   */
  public int getInflightingRPCCounter() {
    return inflightingRPCCounter.get();
  }
}
