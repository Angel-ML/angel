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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
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
    this.lock = new ReentrantLock();
    this.maxPoolSize = maxPoolSize;
  }

  public GenericObjectPool<Channel> getOrCreateChannelPool(Location loc) {
    return getOrCreateChannelPool(loc, maxPoolSize);
  }

  /**
   * Get the channel pool for the server.
   *
   * @param loc server address
   * @return GenericObjectPool<Channel> the channel pool
   */
  public GenericObjectPool<Channel> getOrCreateChannelPool(Location loc, int active) {
    try {
      lock.lock();
      if (!locToChannelPoolMap.containsKey(loc)) {
        locToChannelPoolMap.put(loc, createPool(loc, active));
      }

      return locToChannelPoolMap.get(loc);
    } finally {
      lock.unlock();
    }
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
    return new GenericObjectPool<Channel>(new ChannelObjectFactory(loc, bootstrap), poolConfig);
  }

  /**
   * Refresh the channel pool for the server.
   *
   * @param loc server address
   */
  public void refreshChannelPool(Location loc) {
    try {
      lock.lock();
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

      locToChannelPoolMap.put(loc, createPool(loc, pool.getNumActive() / 5));
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
    try {
      lock.lock();
      GenericObjectPool<Channel> pool = locToChannelPoolMap.remove(loc);
      if (pool != null) {
        try {
          pool.close();
        } catch (Exception e) {
          e.printStackTrace();
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
    try {
      lock.lock();
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
