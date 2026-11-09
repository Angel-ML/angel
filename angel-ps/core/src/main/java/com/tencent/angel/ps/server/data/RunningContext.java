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

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ps.PSContext;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * PS running context
 */
public class RunningContext {

  private static final Log LOG = LogFactory.getLog(RunningContext.class);
  private final ConcurrentHashMap<Integer, ClientRunningContext> clientRPCCounters =
      new ConcurrentHashMap<>();
  private final AtomicLong totalRPCCounter = new AtomicLong(0);
  private final AtomicLong totalRunningRPCCounter = new AtomicLong(0);
  private final AtomicLong oomCounter = new AtomicLong(0);
  private final AtomicLong lastOOMRunningRPCCounter = new AtomicLong(0);
  private final AtomicLong maxRunningRPCCounter = new AtomicLong(0);
  private final AtomicLong generalRunningRPCCounter = new AtomicLong(0);
  private final AtomicLong infligtingRPCCounter = new AtomicLong(0);
  private final AtomicLong network = new AtomicLong(0);
  private final AtomicLong normal = new AtomicLong(0);
  private final AtomicLong requestDataSize = new AtomicLong(0);
  private volatile Thread tokenTimeoutChecker;
  private final int tokenTimeoutMs;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile long lastOOMTs = System.currentTimeMillis();
  private final float genFactor;
  private final AtomicInteger count4RestartSlowPs = new AtomicInteger(0);
  private PSContext context;
  //private final AtomicInteger totalSuccessedRPCCounter = new AtomicInteger(0);
  //private final AtomicLong totalSuccessedRPCUseTime = new AtomicLong(0);
  //private final ConcurrentHashMap<String, Long> requsetIdToUseTimeMap = new ConcurrentHashMap<>();
  /*
  private final ConcurrentHashMap<Integer, PSRPCMetrics> methodIdToMetricsMap = new ConcurrentHashMap<>();

  class PSRPCMetrics {
    private final AtomicInteger rpcCallTime  = new AtomicInteger(0);
    private final AtomicLong totalUseTime = new AtomicLong(0);

    public void add(long useTime) {
      rpcCallTime.incrementAndGet();
      totalUseTime.addAndGet(useTime);
    }

    @Override
    public String toString() {
      if(rpcCallTime.get() == 0) {
        return "PSRPCMetrics{" +
            "rpcCallTime=" + rpcCallTime.get() +
            ", totalUseTime=" + totalUseTime.get() +
            '}';
      } else {
        return "PSRPCMetrics{" +
            "rpcCallTime=" + rpcCallTime.get() +
            ", totalUseTime=" + totalUseTime.get() +
            ", avg use time=" + totalUseTime.get() / rpcCallTime.get() +
            '}';
      }
    }
  }
  */

  public RunningContext(PSContext context) {
    this.context = context;
    LOG.info("Runtime.getRuntime().availableProcessors() = " + Runtime.getRuntime()
        .availableProcessors());
    int workerNum = context.getConf().getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
        AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE);
    float factor = context.getConf()
        .getFloat(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR);

    genFactor = context.getConf()
        .getFloat(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR);

    int serverMem =
        context.getConf().getInt(AngelConf.ANGEL_PS_MEMORY_GB, AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB)
            * 1024;
    int estSize = (int) ((serverMem - 512) * 0.45 / 8);
    //int maxRPCCounter = Math.max(estSize, (int) (workerNum * factor));

    int maxRPCCounter = context.getConf().getInt("angel.ps.max.rpc.counter", 10000);

    maxRunningRPCCounter.set(maxRPCCounter);
    generalRunningRPCCounter.set((int) (maxRPCCounter * genFactor));

    tokenTimeoutMs = context.getConf()
        .getInt(AngelConf.ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS,
            AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS);
  }

  /**
   * Start token timeout checker: if some tokens are not used within a specified time, just release
   * them
   */
  public void start() {
    tokenTimeoutChecker = new Thread(() -> {
      while (!stopped.get() && !Thread.interrupted()) {
        long ts = System.currentTimeMillis();
        for (Map.Entry<Integer, ClientRunningContext> clientEntry : clientRPCCounters.entrySet()) {
          int inflightRPCCounter = clientEntry.getValue().getInflightingRPCCounter();
          long lastUpdateTs = clientEntry.getValue().getLastUpdateTs();
          LOG.debug(
              "inflightRPCCounter=" + inflightRPCCounter + ", lastUpdateTs=" + lastUpdateTs
                  + ", ts="
                  + ts);
          if (inflightRPCCounter != 0 && (ts - lastUpdateTs) > tokenTimeoutMs) {
            LOG.info("client " + clientEntry.getKey() + " token is timeout");
            relaseToken(clientEntry.getKey(), inflightRPCCounter);
          }
        }
        checkOOM();
        if (LOG.isDebugEnabled()) {
          printToken();
        } else {
          printTokenIfBusy();
        }
        printToken();
        // check slow ps for restart
        if (this.context.getConf().getBoolean(AngelConf.ANGEL_PS_SLOWCHECK_ENABLE,
            AngelConf.DEFAULT_ANGEL_PS_SLOWCHECK_ENABLE)) {
          int totalRunningRPCExceededThreshold = this.context.getConf()
              .getInt(AngelConf.ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_THRESHOLD,
                  AngelConf.DEFAULT_ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_THRESHOLD);
          /*
          int averagecostsExceededThreshold = this.context.getConf()
                  .getInt(AngelConf.ANGEL_PS_TOTAL_SUCCESSED_RPC_AVERAGECOSTTIME_EXCEEDED_THRESHOLD,
                          AngelConf.DEFAULT_ANGEL_PS_TOTAL_SUCCESSED_RPC_AVERAGECOSTTIME_EXCEEDED_THRESHOLD);
          double processPerRequestTimeCost = (totalSuccessedRPCCounter.get() > 0) ?
                  totalSuccessedRPCUseTime.get() / (double) totalSuccessedRPCCounter.get() : 0;
                  */
          if (count4RestartSlowPs.get() > this.context.getConf()
              .getInt(AngelConf.ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_COUNT,
                  AngelConf.DEFAULT_ANGEL_PS_TOTAL_RUNNING_RPC_EXCEEDED_COUNT)) {
            LOG.fatal("Now " + this.context.getPs().getServerId() + " is incapable of handling " +
                "rpc request, just failed.");
            System.exit(-1);
          } else {
            if (totalRunningRPCCounter.get() > totalRunningRPCExceededThreshold) {
              count4RestartSlowPs.incrementAndGet();
            } else {
              count4RestartSlowPs.set(0);
            }
          }
          //totalSuccessedRPCCounter.set(0);
          //totalSuccessedRPCUseTime.set(0);
        }
        try {
          Thread.sleep(30000);
        } catch (InterruptedException e) {
          if (!stopped.get()) {
            LOG.error("token-timeout-checker is interrupted");
          }
        }
      }
    });

    tokenTimeoutChecker.setName("token-timeout-checker");
    tokenTimeoutChecker.start();
  }

  private void printTokenIfBusy() {
    if (getState() == ServerState.BUSY) {
      printToken();
    }
  }

  public void increaseNetworkError() {
    network.incrementAndGet();
  }

  public void increaseNormal() {
    normal.incrementAndGet();
  }

  public void increaseRequestDataSize(long size) {
    requestDataSize.addAndGet(size);
  }

  public void increaseResponseDataSize(long size) {
    // Merge request/response size now
    requestDataSize.addAndGet(size);
  }

  public long getRequestDataSize() {
    return requestDataSize.get();
  }

  /**
   * Print context
   */
  public void printToken() {
    LOG.info("=====================Server running context start=======================");
    LOG.info("state = " + getState());
    LOG.info("totalRunningRPCCounter = " + totalRunningRPCCounter.get());
    LOG.info("infligtingRPCCounter = " + infligtingRPCCounter.get());
    LOG.info("oomCounter = " + oomCounter.get());
    LOG.info("maxRunningRPCCounter = " + maxRunningRPCCounter.get());
    LOG.info("generalRunningRPCCounter = " + generalRunningRPCCounter.get());
    LOG.info("lastOOMRunningRPCCounter = " + lastOOMRunningRPCCounter.get());
    LOG.info("totalRPCCounter = " + totalRPCCounter.get());
    //for (Map.Entry<Integer, ClientRunningContext> clientEntry : clientRPCCounters.entrySet()) {
    //  LOG.info("client " + clientEntry.getKey() + " running context:");
    //  clientEntry.getValue().printToken();
    //}

    LOG.info("normal=" + normal.get());
    LOG.info("network=" + network.get());

    /*
    for(Entry<Integer, PSRPCMetrics> metricEntry : methodIdToMetricsMap.entrySet()) {
      LOG.info("Method " + TransportMethod.valueOf(metricEntry.getKey()) + " use time " + metricEntry.getValue());
    }
    */

    LOG.info("=====================Server running context end  =======================");
  }

  /**
   * Stop token timeout checker
   */
  public void stop() {
    if (!stopped.compareAndSet(false, true)) {
      if (tokenTimeoutChecker != null) {
        tokenTimeoutChecker.interrupt();
        tokenTimeoutChecker = null;
      }
    }
  }

  /**
   * Before handle a request, update counters
   *
   * @param clientId client id
   * @param seqId request id
   */
  public void before(int clientId, int seqId) {
    totalRPCCounter.incrementAndGet();
    totalRunningRPCCounter.incrementAndGet();
    getClientRunningContext(clientId).before(seqId);
  }

  /**
   * After handle a request, update counters
   *
   * @param clientId client id
   * @param seqId request id
   */
  public void after(int clientId, int seqId) {
    totalRunningRPCCounter.decrementAndGet();
    getClientRunningContext(clientId).after(seqId);
    if (totalRunningRPCCounter.get() + infligtingRPCCounter.get() < 0.7 * lastOOMRunningRPCCounter
        .get()) {
      oomCounter.set(0);
    }
  }

  private void checkOOM() {
    if (totalRunningRPCCounter.get() + infligtingRPCCounter.get() < 0.7 * lastOOMRunningRPCCounter
        .get()) {
      oomCounter.set(0);
    }
  }

  /**
   * OOM happened
   */
  public void oom() {
    oomCounter.incrementAndGet();
    long runningAndInfightingRPCCounter = getRunningAndInflightingRPCCounter();
    lastOOMRunningRPCCounter.set(runningAndInfightingRPCCounter);
    maxRunningRPCCounter.set((int) (runningAndInfightingRPCCounter * 0.8));
    generalRunningRPCCounter.set((int) (runningAndInfightingRPCCounter * 0.8 * genFactor));
    LOG.info("OOM happened, lastOOMRunningRPCCounter=" + lastOOMRunningRPCCounter.get()
        + ", maxRunningRPCCounter=" + maxRunningRPCCounter.get() + ", generalRunningRPCCounter="
        + generalRunningRPCCounter.get());
  }

  /**
   * Is OOM happened
   *
   * @return true means happened
   */
  public boolean isOOM() {
    return oomCounter.get() > 0;
  }

  /**
   * Get total running rpc number
   *
   * @return total running rpc number
   */
  public long getTotalRunningRPCCounter() {
    return totalRunningRPCCounter.get();
  }

  /**
   * Get Server running state
   *
   * @return server running state
   */
  public ServerState getState() {
    //return ServerState.GENERAL;
    long runningAndInfightingRPCCounter = getRunningAndInflightingRPCCounter();
    if (isOOM()) {
      return ServerState.BUSY;
    }

    if (runningAndInfightingRPCCounter >= maxRunningRPCCounter.get()) {
      return ServerState.BUSY;
    } else if ((runningAndInfightingRPCCounter < maxRunningRPCCounter.get()) && (
        runningAndInfightingRPCCounter >= generalRunningRPCCounter.get())) {
      return ServerState.GENERAL;
    } else {
      return ServerState.IDLE;
    }
  }

  private long getRunningAndInflightingRPCCounter() {
    return totalRunningRPCCounter.get() + infligtingRPCCounter.get();
  }

  /**
   * Allocate token for a request
   *
   * @param clientId client id
   * @param dataSize request size
   * @return token number
   */
  public int allocateToken(int clientId, int dataSize) {
    if (isOOM()) {
      return 0;
    } else {
      long runningAndInfightingRPCCounter = getRunningAndInflightingRPCCounter();
      if (maxRunningRPCCounter.get() - runningAndInfightingRPCCounter >= 1) {
        infligtingRPCCounter.incrementAndGet();
        getClientRunningContext(clientId).allocateToken(1);
        return 1;
      } else {
        return 0;
      }
    }
  }

  /**
   * Release token
   *
   * @param clientId client id
   * @param tokenNum token number
   */
  public void relaseToken(int clientId, int tokenNum) {
    infligtingRPCCounter.addAndGet(-tokenNum);
    getClientRunningContext(clientId).releaseToken(tokenNum);
  }

  /**
   * Get client running context
   *
   * @param clientId client id
   * @return client running context
   */
  public ClientRunningContext getClientRunningContext(int clientId) {
    ClientRunningContext clientContext = clientRPCCounters.get(clientId);
    if (clientContext == null) {
      clientContext = clientRPCCounters.putIfAbsent(clientId, new ClientRunningContext());
      if (clientContext == null) {
        clientContext = clientRPCCounters.get(clientId);
      }
    }
    return clientContext;
  }

  public long getTotalRPCCounter() {
    return totalRPCCounter.get();
  }

  /*
  public void methodUseTime(int methodId, long useTime) {
    PSRPCMetrics metrics = methodIdToMetricsMap.get(methodId);
    if(metrics == null) {
      metrics = methodIdToMetricsMap.putIfAbsent(methodId, new PSRPCMetrics());
      if(metrics == null) {
        metrics = methodIdToMetricsMap.get(methodId);
      }
    }
    metrics.add(useTime);
  }
  */
}
