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

package com.tencent.angel.common.transport;

import com.tencent.angel.common.location.Location;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import io.netty.channel.ChannelFuture;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * Netty channel manager. It manager a channel pool for each server.
 */
public class ChannelManager {

  private static final Log LOG = LogFactory.getLog(ChannelManager.class);
  private final Lock lock;
  /**
   * netty client bootstrap
   */
  private final Bootstrap bootstrap;

  /**
   * server address to channel pool map
   */
  private final HashMap<Location, GenericObjectPool<Channel>> locToChannelPoolMap;

  /**
   * Location to channel map
   */
  private final HashMap<Location, Channel> locToChannelMap;

  private final int maxPoolSize;

  /**
   * Create a new ChannelManager.
   *
   * @param bootstrap netty client bootstrap
   */
  public ChannelManager(Bootstrap bootstrap) {
   this(bootstrap,5);
  }

  /**
   * Create a new ChannelManager.
   *
   * @param bootstrap netty client bootstrap
   */
  public ChannelManager(Bootstrap bootstrap, int maxPoolSize) {
    this.bootstrap = bootstrap;
    this.locToChannelPoolMap = new HashMap<Location, GenericObjectPool<Channel>>();
    this.locToChannelMap = new HashMap<>();
    this.lock = new ReentrantLock();
    this.maxPoolSize = maxPoolSize;
  }

  /**
   * Get the channel pool for the location, if the pool does not exist, create a new for it
   * @param loc server location
   * @return channel pool
   */
  public GenericObjectPool<Channel> getOrCreateChannelPool(Location loc) {
    return getOrCreateChannelPool(loc, maxPoolSize);
  }

  /**
   * Get the channel for the location, if the pool does not exist, create a new for it
   * @param loc server location
   * @return channel
   */
  public Channel getOrCreateChannel(Location loc) throws Exception {
    try {
      lock.lock();
      if (!locToChannelMap.containsKey(loc)) {
        locToChannelMap.put(loc, createChannel(loc));
      }

      return locToChannelMap.get(loc);
    } finally {
      lock.unlock();
    }
  }

  /**
   * Release channel for server
   * @param loc server location
   */
  public void releaseChannel(Location loc) {
    try {
      lock.lock();
      Channel channel = locToChannelMap.get(loc);
      if(channel != null) {
        channel.close();
      }
      locToChannelMap.remove(loc);
    } finally {
      lock.unlock();
    }
  }

  private Channel createChannel(Location loc) throws Exception {
    ChannelFuture connectFuture =
      bootstrap.connect(loc.getIp(), loc.getPort());
    int ticks = 10000;
    while (ticks-- > 0) {
      if (connectFuture.isDone()) {
        return connectFuture.channel();
      }
      Thread.sleep(10);
    }

    if (!connectFuture.isDone()) {
      throw new TimeoutException("connect " + loc + " timeout");
    } else {
      return connectFuture.channel();
    }
  }

  /**
   * Get the channel pool for the server.
   *
   * @param loc server address
   * @return GenericObjectPool<Channel> the channel pool
   */
  public GenericObjectPool<Channel> getOrCreateChannelPool(Location loc, int active) {
    GenericObjectPool<Channel> result;
    lock.lock();

    try {
      if (!locToChannelPoolMap.containsKey(loc)) {
        locToChannelPoolMap.put(loc, createPool(loc, active));
      }
      result = locToChannelPoolMap.get(loc);
    } finally {
      lock.unlock();
    }

    return result;
  }

  private GenericObjectPool<Channel> createPool(Location loc, int active) {
    GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
    poolConfig.maxActive = active * 5;
    poolConfig.maxWait = 30000;
    poolConfig.maxIdle = active * 5;
    poolConfig.minIdle = active;
    poolConfig.testOnBorrow = false;
    poolConfig.testOnReturn = false;
    poolConfig.minEvictableIdleTimeMillis = Integer.MAX_VALUE;
    poolConfig.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    return new GenericObjectPool<>(new ChannelObjectFactory(loc, bootstrap), poolConfig);
  }

  /**
   * Refresh the channel pool for the server.
   *
   * @param loc server address
   */
  public void closeChannelPool(Location loc) {
    try {
      GenericObjectPool<Channel> pool = locToChannelPoolMap.get(loc);
      if(pool == null) {
        return;
      } else {
        try {
          pool.close();
        } catch (Exception e) {
          LOG.error("Close channel for location " + loc +" error ", e);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Remove the channel pool for the server.
   *
   * @param loc server address
   */
  public void removeChannelPool(Location loc) {
    lock.lock();

    try {
      GenericObjectPool<Channel> pool = locToChannelPoolMap.remove(loc);
      if (pool != null) {
        try {
          pool.close();
        } catch (Throwable e) {
          LOG.error("close channel falied ", e);
        }
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Clear and close all channels.
   */
  public void clear() {
    lock.lock();
    try {
      for (Entry<Location, GenericObjectPool<Channel>> entry : locToChannelPoolMap.entrySet()) {
        try {
          entry.getValue().close();
        } catch (Exception e) {
          LOG.error("close channel pool failed, ", e);
        }
      }
      locToChannelPoolMap.clear();
    } finally {
      lock.unlock();
    }
  }
}
