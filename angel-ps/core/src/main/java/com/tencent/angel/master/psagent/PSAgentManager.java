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
package com.tencent.angel.master.psagent;

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
 * PSAgent heartbeat monitor
 */
public class PSAgentManager {
  private static final Log LOG = LogFactory.getLog(PSAgentManager.class);
  private final AMContext context;

  /**psagent attempt id to last heartbeat timestamp map*/
  private final ConcurrentHashMap<Integer, Long> psAgentLastHeartbeatTS;

  /**psagent attempt id to location map*/
  private final ConcurrentHashMap<Integer, Location> psAgentToLocTS;

  /**psagent heartbeat timeout value in millisecond*/
  private final long psAgentTimeOutMS;

  private final AtomicInteger idGen = new AtomicInteger(0);

  public PSAgentManager(AMContext context) {
    this.context = context;
    psAgentLastHeartbeatTS = new ConcurrentHashMap<>();
    psAgentToLocTS = new ConcurrentHashMap<>();
    psAgentTimeOutMS =
      context.getConf().getLong(AngelConf.ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_PSAGENT_HEARTBEAT_TIMEOUT_MS);
  }

  /**
   * PSAgent register
   * @param psAgentId psAgent id
   * @param loc PSAgent location
   */
  public void register(int psAgentId, Location loc) {
    LOG.info("PSAgent " + psAgentId + " is registered in monitor!");
    psAgentLastHeartbeatTS.put(psAgentId, System.currentTimeMillis());
    psAgentToLocTS.put(psAgentId, loc);
  }

  /**
   * PSAgent unregister
   * @param psAgentId psagent id
   */
  public void unRegister(int psAgentId) {
    LOG.info("PSAgent " + psAgentId + " is unregistered in monitor!");
    psAgentLastHeartbeatTS.remove(psAgentId);
  }

  /**
   * Check PSAgent heartbeat timeout or not
   */
  public void checkHBTimeOut() {
    //check whether psagent heartbeat timeout
    Iterator<Map.Entry<Integer, Long>> psAgentIt = psAgentLastHeartbeatTS.entrySet().iterator();
    Map.Entry<Integer, Long> psAgentEntry;
    long currentTs = System.currentTimeMillis();
    while (psAgentIt.hasNext()) {
      psAgentEntry = psAgentIt.next();
      if (currentTs - psAgentEntry.getValue() > psAgentTimeOutMS) {
        LOG.info("removing psagent: " + psAgentEntry.getKey());
        psAgentIt.remove();
      }
    }
  }

  /**
   * Generate a new psagent id
   * @return a new psagent id
   */
  public int getId() {
    return idGen.incrementAndGet();
  }
}
