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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ps.impl;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.transport.ServerState;
import com.tencent.angel.ps.matrix.transport.WorkerPool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * PS running context
 */
public class RunningContext {
  private static final Log LOG = LogFactory.getLog(RunningContext.class);
  private final ConcurrentHashMap<Integer, ClientRunningContext> clientRPCCounters = new ConcurrentHashMap<>();
  private final AtomicInteger totalRPCCounter = new AtomicInteger(0);
  private final AtomicInteger totalRunningRPCCounter = new AtomicInteger(0);
  private final AtomicInteger oomCounter = new AtomicInteger(0);
  private final AtomicInteger lastOOMRunningRPCCounter = new AtomicInteger(0);
  private final AtomicInteger maxRunningRPCCounter = new AtomicInteger(0);
  private final AtomicInteger generalRunningRPCCounter = new AtomicInteger(0);
  private final AtomicInteger infligtingRPCCounter = new AtomicInteger(0);
  private volatile Thread tokenTimeoutChecker;
  private final int tokenTimeoutMs;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile long lastOOMTs = System.currentTimeMillis();
  private final float genFactor;

  public RunningContext(PSContext context) {
    int workerNum = context.getConf().getInt(
      AngelConf.ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_WORKER_POOL_SIZE);
    float factor = context.getConf().getFloat(
      AngelConf.ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_FACTOR);

    genFactor = context.getConf().getFloat(
      AngelConf.ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_RPC_LIMIT_GENERAL_FACTOR);

    int serverMem = context.getConf().getInt(AngelConf.ANGEL_PS_MEMORY_GB,
      AngelConf.DEFAULT_ANGEL_PS_MEMORY_GB) * 1024;
    int estSize = (int)((serverMem - 512) * 0.45 / 8);
    int maxRPCCounter = Math.max(estSize, (int)(workerNum * factor));

    maxRunningRPCCounter.set(maxRPCCounter);
    generalRunningRPCCounter.set((int)(maxRPCCounter * genFactor));

    tokenTimeoutMs = context.getConf().getInt(
      AngelConf.ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS,
      AngelConf.DEFAULT_ANGEL_MATRIXTRANSFER_SERVER_TOKEN_TIMEOUT_MS);
  }

  /**
   * Start token timeout checker: if some tokens are not used within a specified time, just release them
   */
  public void start() {
    tokenTimeoutChecker = new Thread(()-> {
      while(!stopped.get() && !Thread.interrupted()) {
        long ts = System.currentTimeMillis();
        for(Map.Entry<Integer, ClientRunningContext> clientEntry : clientRPCCounters.entrySet()) {
          int inflightRPCCounter = clientEntry.getValue().getInflightingRPCCounter();
          long lastUpdateTs = clientEntry.getValue().getLastUpdateTs();
          LOG.debug("inflightRPCCounter=" + inflightRPCCounter + ", lastUpdateTs=" + lastUpdateTs + ", ts=" + ts);
          if(inflightRPCCounter != 0 && (ts - lastUpdateTs) > tokenTimeoutMs) {
            LOG.info("client " + clientEntry.getKey() + " token is timeout");
            relaseToken(clientEntry.getKey(), inflightRPCCounter);
          }
        }
        checkOOM();
        if(LOG.isDebugEnabled()) {
          printToken();
        }

        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          if(!stopped.get()) {
            LOG.error("token-timeout-checker is interrupted");
          }
        }
      }
    });

    tokenTimeoutChecker.setName("token-timeout-checker");
    tokenTimeoutChecker.start();
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
    for(Map.Entry<Integer, ClientRunningContext> clientEntry : clientRPCCounters.entrySet()) {
      LOG.info("client " + clientEntry.getKey() + " running context:");
      clientEntry.getValue().printToken();
    }

    LOG.info("total=" + WorkerPool.total.get());
    LOG.info("normal=" + WorkerPool.normal);
    LOG.info("network=" + WorkerPool.network);
    LOG.info("channelInUseCounter=" + WorkerPool.channelInUseCounter);
    LOG.info("oom=" + WorkerPool.oom);
    LOG.info("unknown=" + WorkerPool.unknown);
    LOG.info("=====================Server running context end  =======================");
  }

  /**
   * Stop token timeout checker
   */
  public void stop() {
    if(!stopped.compareAndSet(false, true)) {
      if(tokenTimeoutChecker != null) {
        tokenTimeoutChecker.interrupt();
        tokenTimeoutChecker = null;
      }
    }
  }

  /**
   * Before handle a request, update counters
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
   * @param clientId client id
   * @param seqId request id
   */
  public void after(int clientId, int seqId) {
    totalRunningRPCCounter.decrementAndGet();
    getClientRunningContext(clientId).after(seqId);
    if(totalRunningRPCCounter.get() + infligtingRPCCounter.get() < 0.7 * lastOOMRunningRPCCounter.get()) {
      oomCounter.set(0);
    }
  }

  private void checkOOM() {
    if(totalRunningRPCCounter.get() + infligtingRPCCounter.get() < 0.7 * lastOOMRunningRPCCounter.get()) {
      oomCounter.set(0);
    }
  }

  /**
   * OOM happened
   */
  public void oom() {
    oomCounter.incrementAndGet();
    int runningAndInfightingRPCCounter = getRunningAndInflightingRPCCounter();
    lastOOMRunningRPCCounter.set(runningAndInfightingRPCCounter);
    maxRunningRPCCounter.set((int)(runningAndInfightingRPCCounter * 0.8));
    generalRunningRPCCounter.set((int)(runningAndInfightingRPCCounter * 0.8 * genFactor));
    LOG.info("OOM happened, lastOOMRunningRPCCounter=" + lastOOMRunningRPCCounter.get()
      + ", maxRunningRPCCounter=" + maxRunningRPCCounter.get()
      + ", generalRunningRPCCounter=" + generalRunningRPCCounter.get());
  }

  /**
   * Is OOM happened
   * @return true means happened
   */
  public boolean isOOM() {
    return oomCounter.get() > 0;
  }

  /**
   * Get total running rpc number
   * @return total running rpc number
   */
  public int getTotalRunningRPCCounter() {
    return totalRunningRPCCounter.get();
  }

  /**
   * Get Server running state
   * @return server running state
   */
  public ServerState getState() {
    //return ServerState.GENERAL;
    int runningAndInfightingRPCCounter = getRunningAndInflightingRPCCounter();
    if(isOOM()) {
      return ServerState.BUSY;
    }

    if(runningAndInfightingRPCCounter >= maxRunningRPCCounter.get()) {
      return ServerState.BUSY;
    } else if((runningAndInfightingRPCCounter < maxRunningRPCCounter.get()) &&
      (runningAndInfightingRPCCounter >= generalRunningRPCCounter.get())) {
      return ServerState.GENERAL;
    } else {
      return ServerState.IDLE;
    }
  }

  private int getRunningAndInflightingRPCCounter() {
    return totalRunningRPCCounter.get() + infligtingRPCCounter.get();
  }

  /**
   * Allocate token for a request
   * @param clientId client id
   * @param dataSize request size
   * @return token number
   */
  public int allocateToken(int clientId, int dataSize) {
    if(isOOM()) {
      return 0;
    } else {
      int runningAndInfightingRPCCounter = getRunningAndInflightingRPCCounter();
      if(maxRunningRPCCounter.get() - runningAndInfightingRPCCounter >= 1) {
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
   * @param clientId client id
   * @param tokenNum token number
   */
  public void relaseToken(int clientId, int tokenNum) {
    infligtingRPCCounter.addAndGet(-tokenNum);
    getClientRunningContext(clientId).releaseToken(tokenNum);
  }

  /**
   * Get client running context
   * @param clientId client id
   * @return client running context
   */
  public ClientRunningContext getClientRunningContext(int clientId) {
    ClientRunningContext clientContext = clientRPCCounters.get(clientId);
    if(clientContext == null) {
      clientContext = clientRPCCounters.putIfAbsent(clientId, new ClientRunningContext());
      if(clientContext == null) {
        clientContext = clientRPCCounters.get(clientId);
      }
    }
    return clientContext;
  }
}
