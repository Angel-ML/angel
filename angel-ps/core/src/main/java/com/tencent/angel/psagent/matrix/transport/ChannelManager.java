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

package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Netty channel manager. It manager a channel pool for each server.
 */
public class ChannelManager {
  private static final Log LOG = LogFactory.getLog(ChannelManager.class);
  private final Lock lock;
  /**netty client bootstrap*/
  private final Bootstrap bootstrap;
  
  /**server address to channel pool map*/
  private final HashMap<Location, GenericObjectPool<Channel>> locToChannelPoolMap;

  /**
   * Create a new ChannelManager.
   *
   * @param bootstrap netty client bootstrap
   */
  public ChannelManager(Bootstrap bootstrap) {
    this.bootstrap = bootstrap;
    this.locToChannelPoolMap = new HashMap<Location, GenericObjectPool<Channel>>();
    this.lock = new ReentrantLock();
  }

  /**
   * Get the channel pool for the server.
   * 
   * @param loc server address
   * @return GenericObjectPool<Channel> the channel pool
   */
  public GenericObjectPool<Channel> getChannelPool(Location loc) {
    try {
      lock.lock();
      if (!locToChannelPoolMap.containsKey(loc)) {
        locToChannelPoolMap.put(loc, createPool(loc));
      }

      return locToChannelPoolMap.get(loc);
    } finally {
      lock.unlock();
    }
  }

  private GenericObjectPool<Channel> createPool(Location loc) {
    int taskNumber =
        PSAgentContext
            .get()
            .getConf()
            .getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
                AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER);

    GenericObjectPool.Config poolconfig = new GenericObjectPool.Config();
    poolconfig.maxActive = taskNumber * 5;
    poolconfig.maxWait = 10000000;
    poolconfig.maxIdle = taskNumber * 5;
    poolconfig.minIdle = taskNumber;
    poolconfig.testOnBorrow = false;
    poolconfig.testOnReturn = false;
    poolconfig.minEvictableIdleTimeMillis = Integer.MAX_VALUE;
    poolconfig.whenExhaustedAction = GenericObjectPool.WHEN_EXHAUSTED_BLOCK;
    return new GenericObjectPool<Channel>(new ChannelObjectFactory(loc, bootstrap), poolconfig);
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
      if (pool != null) {
        try {
          pool.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }

      locToChannelPoolMap.put(loc, createPool(loc));
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
