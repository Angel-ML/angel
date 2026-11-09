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
          if (LOG.isInfoEnabled()) {
            LOG.info("[CHANNEL-REBUILD][POOL-EMPTY-INIT] loc=" + loc
                + ", timeoutMs=" + timeoutMs + ", maxActive=" + param.maxActive);
          }
          init(timeoutMs);
          if (LOG.isInfoEnabled()) {
            LOG.info("[CHANNEL-REBUILD][POOL-EMPTY-INIT-DONE] loc=" + loc
                + ", poolSize=" + channels.size());
          }
        }
      } finally {
        lock.unlock();
      }
    }

    return channels.get(Math.abs(r.nextInt()) % channels.size());
  }

  private void init(long timeoutMs) {
    if (LOG.isInfoEnabled()) {
      LOG.info("[CHANNEL-POOL][INIT-START] loc=" + loc
          + ", targetSize=" + param.maxActive
          + ", timeoutMs=" + timeoutMs);
    }
    for(int i = 0; i < param.maxActive; i++) {
      channels.add(createChannel(timeoutMs));
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("[CHANNEL-POOL][INIT-DONE] loc=" + loc + ", size=" + channels.size());
    }
  }


  private NettyChannel createChannel(long timeoutMs) {
    Channel newChannel;
    try {
      newChannel = connect(timeoutMs);
      NettyChannel nettyChannel = new NettyChannel(newChannel, loc);
      if (LOG.isInfoEnabled()) {
        LOG.info("[CHANNEL-POOL][CREATE] loc=" + loc + ", channel=" + nettyChannel
            + ", active=" + (newChannel != null && newChannel.isActive())
            + ", open=" + (newChannel != null && newChannel.isOpen()));
      }
      return nettyChannel;
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
    if (LOG.isDebugEnabled()) {
      LOG.debug("[CHANNEL-POOL][CONNECT-START] loc=" + loc + ", timeoutMs=" + timeoutMs);
    }
    ChannelFuture connectFuture = bootstrap.connect(loc.getIp(), loc.getPort());
    int ticks = Math.max(1, (int) (timeoutMs / 10));
    while (ticks-- >= 0) {
      if (connectFuture.isDone()) {
        if (LOG.isInfoEnabled()) {
          Throwable cause = connectFuture.cause();
          LOG.info("[CHANNEL-POOL][CONNECT-DONE] loc=" + loc
              + ", success=" + connectFuture.isSuccess()
              + ", active=" + (connectFuture.channel() != null && connectFuture.channel().isActive())
              + ", open=" + (connectFuture.channel() != null && connectFuture.channel().isOpen())
              + (cause == null ? "" : ", cause=" + cause));
        }
        return connectFuture.channel();
      }
      Thread.sleep(10);
    }

    if (!connectFuture.isDone()) {
      throw new TimeoutException("connect " + loc + " timeout");
    } else {
      if (LOG.isInfoEnabled()) {
        Throwable cause = connectFuture.cause();
        LOG.info("[CHANNEL-POOL][CONNECT-DONE] loc=" + loc
            + ", success=" + connectFuture.isSuccess()
            + ", active=" + (connectFuture.channel() != null && connectFuture.channel().isActive())
            + ", open=" + (connectFuture.channel() != null && connectFuture.channel().isOpen())
            + (cause == null ? "" : ", cause=" + cause));
      }
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
      if (LOG.isInfoEnabled()) {
        LOG.info("[CHANNEL-POOL][REMOVE-ALL] loc=" + loc + ", sizeBefore=" + size);
      }
      for (int i = 0; i < size; i++) {
        channels.get(i).close();
      }
      channels.clear();
    } finally {
      lock.unlock();
    }
  }


  /**
   * Remove a single channel from the pool (e.g. when the caller has just observed it to be
   * inactive). The underlying netty channel is closed. Subsequent {@link #getChannel} calls
   * will lazily re-init the pool if it ends up empty.
   */
  public void removeChannel(NettyChannel target) {
    if (target == null) {
      return;
    }
    try {
      lock.lock();
      boolean removed = channels.remove(target);
      if (removed) {
        target.close();
      }
      if (LOG.isInfoEnabled()) {
        LOG.info("[CHANNEL-REBUILD][EVICT-ONE] loc=" + loc
            + ", removed=" + removed
            + ", target=" + target
            + ", remaining=" + channels.size());
      }
    } finally {
      lock.unlock();
    }
  }

  /**
   * Check the channels in the pool:
   * <ol>
   *   <li>If the underlying netty channel is no longer active/open, evict it and proactively
   *       reconnect a replacement so the pool stays at its configured size. This is what
   *       turns silent middle-box reaps into "noticed at most one check interval later"
   *       instead of "noticed only when a request happens to draw the dead channel".</li>
   *   <li>If the channel has been idle longer than {@code maxIdleTimeMs} and the pool is
   *       above {@code minActive}, close and remove it (original behaviour).</li>
   * </ol>
   * Failures while reconnecting are logged at WARN and the slot is left empty — the next
   * {@link #getChannel} call will lazily re-init the pool if it becomes empty.
   */
  public void check() {
    long ts = System.currentTimeMillis();
    try {
      lock.lock();
      int size = channels.size();
      for (int i = 0; i < size; ) {
        NettyChannel nc = channels.get(i);

        // (1) Dead-channel replacement.
        if (nc.getChannel() == null || !nc.getChannel().isActive() || !nc.getChannel().isOpen()) {
          LOG.warn("channel " + nc + " to " + loc + " is dead, will replace"
              + ", active=" + (nc.getChannel() != null && nc.getChannel().isActive())
              + ", open=" + (nc.getChannel() != null && nc.getChannel().isOpen())
              + ", poolSizeBefore=" + size);
          nc.close();
          channels.remove(i);
          size = channels.size();
          try {
            NettyChannel fresh = createChannel(param.getChannelTimeoutMs);
            channels.add(i, fresh);
            size = channels.size();
            if (LOG.isInfoEnabled()) {
              LOG.info("[CHANNEL-REBUILD][CHECK-REPLACE-SUCCESS] loc=" + loc
                  + ", old=" + nc + ", fresh=" + fresh + ", poolSizeNow=" + size);
            }
            i++;
          } catch (Throwable t) {
            // Leave the slot empty; getChannel() will lazily re-init the pool if it goes empty.
            LOG.warn("[CHANNEL-REBUILD][CHECK-REPLACE-FAILED] recreate channel to " + loc
                + " failed, slot left empty for now, poolSizeNow=" + size, t);
          }
          continue;
        }

        // (2) Idle-channel eviction (original behaviour).
        if (channels.size() > param.minActive && nc.isUseable()
            && (ts - nc.getLastUseTs()) > param.maxIdleTimeMs) {
          LOG.info("channel " + nc + " will be closed, as it not use over "
              + (ts - nc.getLastUseTs()) + " ms");
          nc.close();
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
