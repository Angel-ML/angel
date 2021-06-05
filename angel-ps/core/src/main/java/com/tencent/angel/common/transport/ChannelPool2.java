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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Channel pool to a server
 */
public class ChannelPool2 {
  private static final Log LOG = LogFactory.getLog(ChannelPool.class);
  /**
   * Active channels in the pool
   */
  private final List<NettyChannel> channels;

  /**
   * Netty client bootstrap
   */
  private final Bootstrap bootstrap;

  /**
   * Server location
   */
  private final Location loc;

  /**
   * Pool parameters
   */
  private final ChannelPoolParam param;

  /**
   * Lock
   */
  private final Lock lock;

  private final Random r = new Random();

  /**
   * Create channel token
   */
  private int channelToken;

  /**
   * Create a channel pool
   *
   * @param bootstrap netty client bootstrap
   * @param loc       server location
   * @param param     pool parameters
   */
  public ChannelPool2(Bootstrap bootstrap, Location loc, ChannelPoolParam param) {
    this.bootstrap = bootstrap;
    this.loc = loc;
    this.param = param;
    this.channels = new Vector<>();
    this.lock = new ReentrantLock();
    this.channelToken = 0;
  }

  /**
   * Get a channel from the pool or create a new channel
   *
   * @param timeoutMs max wait time for a channel
   * @return the channel to the server
   * @throws InterruptedException
   * @throws TimeoutException
   */
  public NettyChannel getChannel(long timeoutMs) {
    if(channels.isEmpty()) {
      lock.lock();
      try {
        if(channels.isEmpty()) {
          init(timeoutMs);
        }
      } finally {
        lock.unlock();
      }
    }

    return channels.get(Math.abs(r.nextInt()) % channels.size());
  }

  private void init(long timeoutMs) {
    for(int i = 0; i < param.maxActive; i++) {
      channels.add(createChannel(timeoutMs));
    }
  }

  private NettyChannel createChannel(long timeoutMs) {
    Channel newChannel;
    try {
      newChannel = connect(timeoutMs);
      return new NettyChannel(newChannel, loc);
    } catch (Throwable e) {
      LOG.error("Connect to " + loc + " failed ", e);
      throw new RuntimeException("Connect to " + loc + " failed ", e);
    }
  }

  /**
   * Connect to the server to get a channel
   *
   * @param timeoutMs max wait time
   * @return a channel to the server
   * @throws InterruptedException
   * @throws TimeoutException
   */
  private Channel connect(long timeoutMs) throws InterruptedException, TimeoutException {
    ChannelFuture connectFuture = bootstrap.connect(loc.getIp(), loc.getPort());
    int ticks = Math.max(1, (int) (timeoutMs / 10));
    while (ticks-- >= 0) {
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
   * Release the channel
   *
   * @param channel channel
   */
  public void releaseChannel(NettyChannel channel) {

  }

  /**
   * Remove all channels in the pool
   */
  public void removeChannels() {
    try {
      lock.lock();
      int size = channels.size();
      for (int i = 0; i < size; i++) {
        channels.get(i).close();
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Check the idle times of all channels are over limit or not
   */
  public void check() {
    long ts = System.currentTimeMillis();
    try {
      lock.lock();
      int size = channels.size();
      for (int i = 0; i < size; ) {
        if (channels.size() > param.minActive && channels.get(i).isUseable()
            && (ts - channels.get(i).getLastUseTs()) > param.maxIdleTimeMs) {
          LOG.info("channel " + channels.get(i) + " will be closed, as it not use over "
              + (ts - channels.get(i).getLastUseTs()) + " ms");
          channels.get(i).close();
          channels.remove(i);
          size = channels.size();
        } else {
          i++;
        }
      }
    } finally {
      lock.unlock();
    }
  }

  @Override public String toString() {
    StringBuilder sb = new StringBuilder();
    int usableNum = 0;
    try {
      lock.lock();
      int size = channels.size();
      for (int i = 0; i < size; i++) {
        if (channels.get(i).isUseable()) {
          usableNum++;
        }
      }
    } finally {
      lock.unlock();
    }

    sb.append("loc=").append(loc).append(",");
    sb.append("channel number=").append(channels.size()).append(",");
    sb.append("channel usable number=").append(usableNum).append(",");
    sb.append("channelToken=").append(channelToken);
    return sb.toString();
  }
}
