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


package com.tencent.angel.ipc;

import com.tencent.angel.conf.AngelConf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Cache a client using its socket factory as the hash key. Enables reuse/sharing of clients on a
 * per SocketFactory basis. A client establishes certain configuration dependent characteristics
 * like timeouts, tcp-keepalive (true or false), etc. For more details on the characteristics.
 * Creation of dynamic proxies to protocols creates the clients (and increments reference count once
 * created), and stopping of the proxies leads to clearing out references and when the reference
 * drops to zero, the cache mapping is cleared.
 */
class ClientCache {
  private static final Logger LOG = LoggerFactory.getLogger(ClientCache.class.getName());
  private Map<String, NettyTransceiver> clients = new HashMap<String, NettyTransceiver>();
  private Configuration conf = new Configuration();

  private final Class<? extends Channel> socketChannelClass;
  private EventLoopGroup workerGroup;
  private PooledByteBufAllocator pooledAllocator;

  protected ClientCache() {
    int nThreads =
      conf.getInt(AngelConf.CLIENT_IO_THREAD, Runtime.getRuntime().availableProcessors() * 2);
    IOMode ioMode = IOMode.valueOf(conf.get(AngelConf.NETWORK_IO_MODE, "NIO"));
    workerGroup = NettyUtils.createEventLoop(ioMode, nThreads, "ML-client");
    pooledAllocator = NettyUtils.createPooledByteBufAllocator(true, true, nThreads);
    socketChannelClass = NettyUtils.getClientChannelClass(ioMode);
  }

  /**
   * Construct & cache an IPC client with the user-provided SocketFactory if no cached client
   * exists.
   *
   * @param conf    Configuration
   * @param factory socket factory
   * @return an IPC client
   */
  protected synchronized NettyTransceiver getClient(InetSocketAddress addr, SocketFactory factory,
    Configuration conf) {
    NettyTransceiver client = clients.get(addr.toString());
    if (client == null) {
      try {
        int connectTimeoutMillis = (int) conf.getLong(AngelConf.ML_CONNECTION_TIMEOUT_MILLIS,
          AngelConf.DEFAULT_CONNECTION_TIMEOUT_MILLIS);
        client = new NettyTransceiver(conf, addr, workerGroup, pooledAllocator, socketChannelClass,
          connectTimeoutMillis);
      } catch (Exception e) {
        LOG.debug("create NettyTransceiver client error: " + e);
        throw new RuntimeException("create NettyTransceiver client error: ", e);
      }

      clients.put(addr.toString(), client);
    } else {
      client.incCount();
    }
    return client;
  }

  /**
   * Stop a RPC client connection A RPC client is closed only when its reference count becomes zero.
   *
   * @param client client to stop
   */
  protected void stopClient(NettyTransceiver client) {
    synchronized (this) {
      client.decCount();
      if (client.isZeroReference()) {
        clients.remove(client.getRemoteAddr().toString());
      }
    }
    if (client.isZeroReference()) {
      client.close();
    }
  }

  /**
   * Clear all netty client in cache.
   */
  public void clear() {
    synchronized (this) {
      Iterator<Map.Entry<String, NettyTransceiver>> iter = clients.entrySet().iterator();
      while (iter.hasNext()) {
        NettyTransceiver client = iter.next().getValue();
        client.close();
        iter.remove();
      }
    }
  }
}
