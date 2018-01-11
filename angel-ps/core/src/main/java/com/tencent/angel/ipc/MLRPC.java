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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Add shutDown method to fix Angel client exit problem.
 */
package com.tencent.angel.ipc;

import com.tencent.angel.exception.RetriesExhaustedException;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.utils.ReflectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@SuppressWarnings("rawtypes")
public class MLRPC {

  protected static final Logger LOG = LoggerFactory.getLogger(MLRPC.class);

  /**
   * Configuration key for the {@link RpcEngine} implementation to load to handle connection
   * protocols. Handlers for individual protocols can be configured using
   * {@code "ml.rpc.engine." + protocol.class.name}.
   */
  public static final String RPC_ENGINE_PROP = "ml.rpc.engine";

  // track what RpcEngine is used by a proxy class, for stopProxy()
  private static final Map<Class, RpcEngine> PROXY_ENGINES = new HashMap<Class, RpcEngine>();

  // cache of RpcEngines by protocol
  private static final Map<Class, RpcEngine> PROTOCOL_ENGINES = new HashMap<Class, RpcEngine>();

  /** Construct a server for a protocol implementation instance. */
  @SuppressWarnings("unchecked")
  public static RpcServer getServer(Class protocol, final Object instance, final Class<?>[] ifaces,
      String bindAddress, int port, Configuration conf) throws IOException {
    return getProtocolEngine(protocol, conf).getServer(protocol, instance, ifaces, bindAddress,
        port, conf);
  }

  // return the RpcEngine configured to handle a protocol
  private static synchronized RpcEngine getProtocolEngine(Class protocol, Configuration conf) {
    RpcEngine engine = PROTOCOL_ENGINES.get(protocol);
    if (engine == null) {
      Class<?> defaultEngine = ProtobufRpcEngine.class;
      Class<?> impl = defaultEngine;
      LOG.debug("Using " + impl.getName() + " for " + protocol.getName());
      engine = (RpcEngine) ReflectionUtils.newInstance(impl, conf);
      if (protocol.isInterface())
        PROXY_ENGINES.put(Proxy.getProxyClass(protocol.getClassLoader(), protocol), engine);
      PROTOCOL_ENGINES.put(protocol, engine);
    }
    return engine;
  }

  /**
   * Construct a client-side proxy object that implements the named protocol, talking to a server at
   * the named address.
   * 
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param conf configuration
   * @param factory socket factory
   * @param rpcTimeout timeout for each RPC
   * @return proxy
   * @throws java.io.IOException e
   */
  public static VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf, SocketFactory factory,
      int rpcTimeout, List<String> addrList4Failover) throws IOException {
    RpcEngine engine = getProtocolEngine(protocol, conf);
    VersionedProtocol proxy =
        engine
            .getProxy(protocol, clientVersion, addr, conf, factory, rpcTimeout, addrList4Failover);
    return proxy;
  }

  /**
   * Construct a client-side proxy object with the default SocketFactory
   * 
   * @param protocol interface
   * @param clientVersion version we are expecting
   * @param addr remote address
   * @param conf configuration
   * @param rpcTimeout timeout for each RPC
   * @return a proxy instance
   * @throws java.io.IOException e
   */
  public static VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol,
      long clientVersion, InetSocketAddress addr, Configuration conf, int rpcTimeout,
      List<String> addrList4Failover) throws IOException {

    return getProxy(protocol, clientVersion, addr, conf, NetUtils.getDefaultSocketFactory(conf),
        rpcTimeout, addrList4Failover);
  }

  static long getProtocolVersion(Class<? extends VersionedProtocol> protocol)
      throws NoSuchFieldException, IllegalAccessException {
    Field versionField = protocol.getField("VERSION");
    versionField.setAccessible(true);
    return versionField.getLong(protocol);
  }

  /**
   * @param protocol protocol interface
   * @param clientVersion which client version we expect
   * @param addr address of remote service
   * @param conf configuration
   * @param maxAttempts max attempts
   * @param rpcTimeout timeout for each RPC
   * @param timeout timeout in milliseconds
   * @return proxy
   * @throws java.io.IOException e
   */
  @SuppressWarnings("unchecked")
  public static VersionedProtocol waitForProxy(Class protocol, long clientVersion,
      InetSocketAddress addr, Configuration conf, int maxAttempts, int rpcTimeout, long timeout,
      List<String> addrList4Failover) throws IOException {
    long startTime = System.currentTimeMillis();
    IOException ioe;
    int reconnectAttempts = 0;
    while (true) {
      try {
        return getProxy(protocol, clientVersion, addr, conf, rpcTimeout, addrList4Failover);
      } catch (SocketTimeoutException te) { // namenode is busy
        LOG.info("Problem connecting to server: " + addr, te);
        ioe = te;
      } catch (IOException ioex) {
        // We only handle the ConnectException.
        LOG.info("Get proxy failed!", ioex);
        ConnectException ce = null;
        if (ioex instanceof ConnectException) {
          ce = (ConnectException) ioex;
          ioe = ce;
        } else if (ioex.getCause() != null && ioex.getCause() instanceof ConnectException) {
          ce = (ConnectException) ioex.getCause();
          ioe = ce;
        } else if (ioex.getMessage().toLowerCase().contains("connection refused")) {
          ce = new ConnectException(ioex.getMessage());
          ioe = ce;
        } else {
          // This is the exception we can't handle.
          ioe = ioex;
        }
        if (ce != null) {
          handleConnectionException(++reconnectAttempts, maxAttempts, protocol, addr, ce);
        }
      }
      // check if timed out
      if (System.currentTimeMillis() - timeout >= startTime) {
        throw ioe;
      }

      // wait for retry
      try {
        Thread.sleep(1000);
      } catch (InterruptedException ie) {
        // IGNORE
      }
    }
  }

  /**
   * @param retries current retried times.
   * @param maxAttmpts max attempts
   * @param protocol protocol interface
   * @param addr address of remote service
   * @param ce ConnectException
   * @throws RetriesExhaustedException
   */
  private static void handleConnectionException(int retries, int maxAttmpts, Class<?> protocol,
      InetSocketAddress addr, ConnectException ce) throws RetriesExhaustedException {
    if (maxAttmpts >= 0 && retries >= maxAttmpts) {
      LOG.info("Server at " + addr + " could not be reached after " + maxAttmpts
          + " tries, giving up.");
      throw new RetriesExhaustedException("Failed setting up proxy " + protocol + " to "
          + addr.toString() + " after attempts=" + maxAttmpts, ce);
    }
  }

  public static void stopProxy(VersionedProtocol proxy) {
    if (proxy != null) {
      LOG.debug("stop proxy");
      getProxyEngine(proxy).stopProxy(proxy);
    }
  }

  // return the RpcEngine that handles a proxy object
  private static synchronized RpcEngine getProxyEngine(Object proxy) {
    return PROXY_ENGINES.get(proxy.getClass());
  }

  /**
   * Close all connections and all workers.
   */
  public static synchronized void shutDown() {
    Iterator<Map.Entry<Class, RpcEngine>> iter = PROXY_ENGINES.entrySet().iterator();
    while(iter.hasNext()) {
      iter.next().getValue().shutDown();
      iter.remove();
    }
  }
}
