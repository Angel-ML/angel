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
   * Init and start channel manager
   */
  public void initAndStart() {
    checker = new Thread(new Runnable() {
      @Override public void run() {
        while (!stopped.get() && !Thread.interrupted()) {
          try {
            Thread.sleep(30000);
            for (ChannelPool2 pool : locToChannelPoolMap.values()) {
              pool.check();
            }
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              LOG.error("pools checker is interrupted ", e);
            }
          }
        }
      }
    });

    checker.setName("pools-checker");
    //checker.start();
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
      pool = locToChannelPoolMap.putIfAbsent(loc, new ChannelPool2(bootstrap, loc, poolParam));
      if (pool == null) {
        pool = locToChannelPoolMap.get(loc);
      }
    }

    NettyChannel channel = pool.getChannel(timeoutMs);
    return channel;
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
      pool.removeChannels();
    }
  }

  /**
   * Close all channel pools
   */
  public void clear() {
    for (ChannelPool2 pool : locToChannelPoolMap.values()) {
      pool.removeChannels();
    }
    locToChannelPoolMap.clear();
  }
}