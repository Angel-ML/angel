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

package com.tencent.angel.master.client;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Angel client manager
 */
public class ClientManager {
  private static final Log LOG = LogFactory.getLog(ClientManager.class);
  private final AMContext context;

  /**
   * Client id to last heartbeat timestamp map
   */
  private final ConcurrentHashMap<Integer, Long> clientToLastHBTsMap;

  /**
   * Client id to client location map
   */
  private final ConcurrentHashMap<Integer, Location> clientIdToLocMap;

  /**
   * Client timeout time in milliseconds
   */
  private final long clientTimeoutMS;

  /**
   * Client id generator
   */
  private final AtomicInteger idGen = new AtomicInteger(0);

  public ClientManager(AMContext context) {
    this.context = context;
    clientToLastHBTsMap = new ConcurrentHashMap<>();
    clientIdToLocMap = new ConcurrentHashMap<>();
    clientTimeoutMS =
      context.getConf().getLong(AngelConf.ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_CLIENT_HEARTBEAT_INTERVAL_TIMEOUT_MS);
  }

  /**
   * Check if some client heartbeat timeout, if all client timeout, just kill the application
   */
  public void checkHBTimeOut() {
    Iterator<Map.Entry<Integer, Long>> clientIt = clientToLastHBTsMap.entrySet().iterator();
    boolean isTimeOut = false;
    Map.Entry<Integer, Long> clientEntry;
    long currentTs = System.currentTimeMillis();
    while(clientIt.hasNext()) {
      clientEntry = clientIt.next();
      if (currentTs - clientEntry.getValue() > clientTimeoutMS) {
        LOG.error("Client " + clientEntry.getKey() + " heartbeat timeout");
        clientIt.remove();
        isTimeOut = true;
      }
    }

    if(isTimeOut && clientToLastHBTsMap.isEmpty()) {
      LOG.error("All client timeout, just exit the application");
      context.getMasterService().stop(1);
    }
  }

  /**
   * Is a client alive
   * @param clientId client id
   * @return true means alive
   */
  public boolean isAlive(int clientId) {
    return clientToLastHBTsMap.containsKey(clientId);
  }

  /**
   * Update the heartbeat timestamp for a client
   * @param clientId client id
   */
  public void alive(int clientId) {
    clientToLastHBTsMap.put(clientId, System.currentTimeMillis());
  }

  /**
   * Client register
   * @param clientId client id
   */
  public void register(int clientId) {
    clientToLastHBTsMap.put(clientId, System.currentTimeMillis());
  }

  /**
   * Client unregister
   * @param clientId client id
   */
  public void unRegister(int clientId) {
    clientToLastHBTsMap.remove(clientId);
  }

  /**
   * Generate a new client id
   * @return a new client id
   */
  public int getId() {
    return idGen.incrementAndGet();
  }
}
