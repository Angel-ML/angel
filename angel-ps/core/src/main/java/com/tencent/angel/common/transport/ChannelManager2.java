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


package com.tencent.angel.common.transport;

import com.tencent.angel.common.location.Location;
import io.netty.bootstrap.Bootstrap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A simple netty channel manager
 */
public class ChannelManager2 {
  private static final Log LOG = LogFactory.getLog(ChannelManager2.class);
  /**
   * netty client bootstrap
   */
  private final Bootstrap bootstrap;

  /**
   * Channel pool parameters
   */
  private final ChannelPoolParam poolParam;

  /**
   * Location to netty channel pool map
   */
  private final ConcurrentHashMap<Location, ChannelPool2> locToChannelPoolMap;

  /**
   * Check thread
   */
  private volatile Thread checker;

  /**
   * Background pool check interval in milliseconds. Default 30s. The checker walks every
   * {@link ChannelPool2} and (1) replaces channels whose underlying netty {@link
   * io.netty.channel.Channel} is no longer active/open, and (2) closes channels that have
   * been idle beyond {@link ChannelPoolParam#maxIdleTimeMs}. Without this thread, dead
   * long-lived TCP sessions (reaped by middleboxes while the application is silent) are only
   * discovered on the very request that next happens to draw them, which manifests as the
   * "channel ... is not active" error storm at the application layer.
   */
  private long checkIntervalMs = 30_000L;

  private final AtomicBoolean stopped = new AtomicBoolean(false);


  /**
   * Create a new ChannelManager2
   *
   * @param bootstrap           netty client bootstrap
   * @param maxActive           max active channel number to a server
   * @param minActive           min active channel number to a server
   * @param maxIdleTimeMs       the max idle time for a channel, if over this time, the channel will be closed
   * @param getChannelTimeoutMs max get channel time in milliseconds
   */
  public ChannelManager2(Bootstrap bootstrap, int maxActive, int minActive, long maxIdleTimeMs,
    long getChannelTimeoutMs) {
    this.bootstrap = bootstrap;
    poolParam = new ChannelPoolParam();
    poolParam.maxActive = maxActive;
    poolParam.minActive = minActive;
    poolParam.maxIdleTimeMs = maxIdleTimeMs;
    poolParam.getChannelTimeoutMs = getChannelTimeoutMs;
    locToChannelPoolMap = new ConcurrentHashMap<>();
  }

  /**
   * Create a new ChannelManager2
   *
   * @param bootstrap netty client bootstrap
   * @param poolParam Netty channel pool parameters
   */
  public ChannelManager2(Bootstrap bootstrap, ChannelPoolParam poolParam) {
    this.bootstrap = bootstrap;
    this.poolParam = poolParam;
    locToChannelPoolMap = new ConcurrentHashMap<>();
  }

  /**
   * Create a new ChannelManager2
   *
   * @param bootstrap netty client bootstrap
   * @param maxActive max active channel number to a server
   */
  public ChannelManager2(Bootstrap bootstrap, int maxActive) {
    this(bootstrap, maxActive, 0, 60000, 10000);
  }

  /**
   * Configure the background pool check interval. Must be called before {@link #initAndStart()}.
   */
  public void setCheckIntervalMs(long checkIntervalMs) {
    if (checkIntervalMs > 0) {
      this.checkIntervalMs = checkIntervalMs;
    }
  }

  /**
   * Init and start channel manager. The background checker is now started by default — it is
   * the only mechanism that proactively evicts and re-creates dead long-lived channels (see
   * {@link ChannelPool2#check()}); historically it was constructed but never started, which is
   * a primary contributor to the "channel ... is not active" storms seen in production.
   */
  public void initAndStart() {
    final long intervalMs = checkIntervalMs;
    if (LOG.isInfoEnabled()) {
      LOG.info("[CHANNEL-MANAGER][START] intervalMs=" + intervalMs
          + ", maxActive=" + poolParam.maxActive
          + ", minActive=" + poolParam.minActive
          + ", maxIdleTimeMs=" + poolParam.maxIdleTimeMs
          + ", getChannelTimeoutMs=" + poolParam.getChannelTimeoutMs);
    }
    checker = new Thread(new Runnable() {

      @Override public void run() {
        while (!stopped.get() && !Thread.interrupted()) {
          try {
            Thread.sleep(intervalMs);
          } catch (InterruptedException e) {
            if (stopped.get()) {
              return;
            }
            LOG.warn("pools checker is interrupted, continue");
            continue;
          }
          try {
            for (ChannelPool2 pool : locToChannelPoolMap.values()) {
              pool.check();
            }
          } catch (Throwable t) {
            LOG.warn("pools checker iteration failed, will retry next tick", t);
          }
        }
      }
    });

    checker.setName("pools-checker");
    checker.setDaemon(true);
    checker.start();
  }

  /**
   * Stop the channel manager
   */
  public void stop() {
    if (stopped.getAndSet(true)) {
      return;
    }

    clear();
    if (checker != null) {
      checker.interrupt();
    }
    LOG.info("Channel manager stop");
  }

  /**
   * Get a channel to a server
   *
   * @param loc server location
   * @return channel to this server
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public NettyChannel getChannel(Location loc) throws TimeoutException, InterruptedException {
    return getChannel(loc, poolParam.getChannelTimeoutMs);
  }

  /**
   * Get a channel to a server
   *
   * @param loc       server location
   * @param timeoutMs max get time in milliseconds
   * @return channel to this server
   * @throws TimeoutException
   * @throws InterruptedException
   */
  public NettyChannel getChannel(Location loc, long timeoutMs) {
    ChannelPool2 pool = locToChannelPoolMap.get(loc);
    if (pool == null) {
      ChannelPool2 newPool = new ChannelPool2(bootstrap, loc, poolParam);
      pool = locToChannelPoolMap.putIfAbsent(loc, newPool);
      if (pool == null) {
        pool = newPool;
        if (LOG.isInfoEnabled()) {
          LOG.info("[CHANNEL-MANAGER][POOL-CREATE] loc=" + loc
              + ", timeoutMs=" + timeoutMs
              + ", totalPools=" + locToChannelPoolMap.size());
        }
      }
    }

    return pool.getChannel(timeoutMs);
  }


  /**
   * Release the channel
   *
   * @param channel the channel need released
   */
  public void releaseChannel(NettyChannel channel) {
    ChannelPool2 pool = locToChannelPoolMap.get(channel.getLoc());
    if (pool != null) {
      pool.releaseChannel(channel);
    }
  }

  /**
   * Remove the channel pool for the server.
   *
   * @param loc server address
   */
  public void removeChannels(Location loc) {
    ChannelPool2 pool = locToChannelPoolMap.remove(loc);
    if (pool != null) {
      if (LOG.isInfoEnabled()) {
        LOG.info("[CHANNEL-MANAGER][POOL-REMOVE] loc=" + loc
            + ", totalPoolsAfterRemove=" + locToChannelPoolMap.size());
      }
      pool.removeChannels();
    } else if (LOG.isDebugEnabled()) {
      LOG.debug("[CHANNEL-MANAGER][POOL-REMOVE-SKIP] loc=" + loc + " not found");
    }
  }


  /**
   * Evict a single dead channel from its pool. Used by callers that observed the channel to be
   * inactive on borrow — eviction is done synchronously so the next borrow on the same pool
   * cannot re-pick the same dead entry. The pool will lazily re-init if it ends up empty.
   */
  public void removeChannel(NettyChannel channel) {
    if (channel == null) {
      return;
    }
    ChannelPool2 pool = locToChannelPoolMap.get(channel.getLoc());
    if (pool != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("[CHANNEL-MANAGER][CHANNEL-EVICT] loc=" + channel.getLoc()
            + ", channel=" + channel);
      }
      pool.removeChannel(channel);
    }
  }


  /**
   * Close all channel pools
   */
  public void clear() {
    int poolNum = locToChannelPoolMap.size();
    for (ChannelPool2 pool : locToChannelPoolMap.values()) {
      pool.removeChannels();
    }
    locToChannelPoolMap.clear();
    if (LOG.isInfoEnabled()) {
      LOG.info("[CHANNEL-MANAGER][CLEAR] removedPools=" + poolNum);
    }
  }
}