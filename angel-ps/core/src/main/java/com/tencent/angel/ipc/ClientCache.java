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
 * Add clear method to fix Angel client exit problem.
 */

package com.tencent.angel.ipc;

import com.tencent.angel.conf.TConstants;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
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

  protected ClientCache() {}

  /**
   * Construct & cache an IPC client with the user-provided SocketFactory if no cached client
   * exists.
   * 
   * @param conf Configuration
   * @param factory socket factory
   * @return an IPC client
   */
  protected synchronized NettyTransceiver getClient(InetSocketAddress addr, SocketFactory factory,
      Configuration conf) {

    NettyTransceiver client = clients.get(addr.toString());
    if (client == null) {
      Class<? extends NettyTransceiver> mlClientClass = NettyTransceiver.class;

      // Make an ml rpc client.
      try {
        Constructor<? extends NettyTransceiver> cst =
            mlClientClass.getConstructor(InetSocketAddress.class, Long.class);
        client =
            cst.newInstance(addr, conf.getLong(TConstants.ML_CONNECTION_TIMEOUT_MILLIS,
                TConstants.DEFAULT_CONNECTION_TIMEOUT_MILLIS));
        client.setConf(conf);
      } catch (InvocationTargetException e) {
        LOG.debug("create NettryTransceiver client error1: " + e);
        throw new RuntimeException(e);
      } catch (InstantiationException e) {
        LOG.debug("create NettryTransceiver client error2: " + e);
        throw new RuntimeException(e);
      } catch (IllegalAccessException e) {
        LOG.debug("create NettryTransceiver client error3: " + e);
        throw new RuntimeException(e);
      } catch (NoSuchMethodException e) {
        LOG.debug("create NettryTransceiver client error4: " + e);
        throw new RuntimeException("No matching constructor in " + mlClientClass.getName(), e);
      } catch (Exception e) {
        LOG.debug("create NettryTransceiver client error: " + e);
        throw new RuntimeException("create NettryTransceiver client error: ", e);
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
