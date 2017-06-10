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

import com.tencent.angel.RunningMode;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.psagent.PSAgentId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

public class PSAgentManager implements EventHandler<PSAgentManagerEvent> {
  private static final Log LOG = LogFactory.getLog(PSAgentManager.class);
  private final AMContext context; 
  private final Resource psAgentResource;
  private final Priority psAgentPriority = RecordFactoryProvider.getRecordFactory(null)
      .newRecordInstance(Priority.class);
  private final Map<PSAgentId, AMPSAgent> psAgentMap;
  private final Map<PSAgentId, AMPSAgent> successPSAgentMap;
  private final Map<PSAgentId, AMPSAgent> killedPSAgentMap;
  private final Map<PSAgentId, AMPSAgent> failedPSAgentMap;
  private int psAgentNumber;
  private String[] ips;
  private final int maxAttemptNum;

  public PSAgentManager(AMContext context) {
    this.context = context;
    Configuration conf = context.getConf();

    int psAgentMemory =
        conf.getInt(AngelConfiguration.ANGEL_PSAGENT_MERMORY_MB,
            AngelConfiguration.DEFAULT_ANGEL_PSAGENT_MERMORY_MB);
    int psAgentVcores =
        conf.getInt(AngelConfiguration.ANGEL_PSAGENT_CPU_VCORES,
            AngelConfiguration.DEFAULT_ANGEL_PSAGENT_CPU_VCORES);
    int priority =
        conf.getInt(AngelConfiguration.ANGEL_PSAGENT_PRIORITY,
            AngelConfiguration.DEFAULT_ANGEL_PSAGENT_PRIORITY);

    maxAttemptNum =
        conf.getInt(AngelConfiguration.ANGEL_PSAGENT_MAX_ATTEMPTS,
            AngelConfiguration.DEFAULT_ANGEL_PSAGENT_MAX_ATTEMPTS);

    LOG.info("psagent priority = " + priority);
    psAgentResource = Resource.newInstance(psAgentMemory, psAgentVcores);
    psAgentPriority.setPriority(priority);

    psAgentMap = new ConcurrentHashMap<PSAgentId, AMPSAgent>();
    successPSAgentMap = new ConcurrentHashMap<PSAgentId, AMPSAgent>();
    killedPSAgentMap = new ConcurrentHashMap<PSAgentId, AMPSAgent>();
    failedPSAgentMap = new ConcurrentHashMap<PSAgentId, AMPSAgent>();
  }
  
  public void init() throws InvalidParameterException {
    Configuration conf = context.getConf();
    if (context.getRunningMode() == RunningMode.ANGEL_PS_PSAGENT) {
      String ipStr = conf.get(AngelConfiguration.ANGEL_PSAGENT_IPLIST);
      if (ipStr != null) {
        ips = ipStr.split(",");
        psAgentNumber = ips.length;
      } else {
        throw new InvalidParameterException("ip list is null, property "
            + AngelConfiguration.ANGEL_PSAGENT_IPLIST + " must be set");
      }

      initPSAgents();
    } else {
      ips = null;
      psAgentNumber = 0;
    }
  }

  @Override
  public void handle(PSAgentManagerEvent event) {
    switch (event.getType()) {
      case PSAGENTS_START: {
        startPSAgens(event);
        break;
      }

      case PSAGENTS_KILL: {
        killPSAgents(event);
        break;
      }

      case PSAGENTS_FAIL: {
        failPSAgents(event);
        break;
      }

      case PSAGENT_REGISTER: {
        psAgentRegister((PSAgentRegisterEvent) event);
        break;
      }

      case PSAGENT_DONE: {
        psAgentSucess(event);
        break;
      }

      case PSAGENT_KILLED: {
        psAgentKilled(event);
        break;
      }

      case PSAGENT_FAILED: {
        psAgentFalied(event);
        break;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void psAgentRegister(PSAgentRegisterEvent event) {
    AMPSAgent agent = new AMPSAgent(context, event.getPsAgentId(), event.getLocation());
    psAgentMap.put(event.getPsAgentId(), agent);
    context.getEventHandler().handle(
        new PSAgentFromAttemptEvent(AMPSAgentEventType.PSAGENT_ATTEMPT_REGISTERED, event
            .getAttemptId()));
  }

  @SuppressWarnings("unchecked")
  private void psAgentFalied(PSAgentManagerEvent event) {
    failedPSAgentMap.put(event.getPsAgentId(), psAgentMap.get(event.getPsAgentId()));
    List<String> diagnostics =
        context.getPSAgentManager().getPsAgent(event.getPsAgentId()).getDiagnostics();
    StringBuilder diagnostic = new StringBuilder("");
    for (String diagItem : diagnostics) {
      diagnostic.append(diagItem).append("\n");
    }
    context.getEventHandler().handle(
        new InternalErrorEvent(context.getApplicationId(), diagnostic.toString()));
  }

  @SuppressWarnings("unchecked")
  private void psAgentKilled(PSAgentManagerEvent event) {
    killedPSAgentMap.put(event.getPsAgentId(), psAgentMap.get(event.getPsAgentId()));
    context.getEventHandler().handle(new AppEvent(context.getApplicationId(), AppEventType.KILL));
  }

  private void psAgentSucess(PSAgentManagerEvent event) {
    successPSAgentMap.put(event.getPsAgentId(), psAgentMap.get(event.getPsAgentId()));
    if (successPSAgentMap.size() == psAgentMap.size()) {
      LOG.info("all psagent is done, now commit the matries");
    }
  }

  @SuppressWarnings("unchecked")
  private void failPSAgents(PSAgentManagerEvent event) {
    if (event.getPsAgentId() == null) {
      for (Entry<PSAgentId, AMPSAgent> entry : psAgentMap.entrySet()) {
        context.getEventHandler().handle(
            new AMPSAgentEvent(AMPSAgentEventType.PSAGENT_ERROR, entry.getKey()));
      }
    } else {
      context.getEventHandler().handle(
          new AMPSAgentEvent(AMPSAgentEventType.PSAGENT_ERROR, event.getPsAgentId()));
    }
  }

  @SuppressWarnings("unchecked")
  private void killPSAgents(PSAgentManagerEvent event) {
    if (event.getPsAgentId() == null) {
      for (Entry<PSAgentId, AMPSAgent> entry : psAgentMap.entrySet()) {
        context.getEventHandler().handle(
            new AMPSAgentEvent(AMPSAgentEventType.PSAGENT_KILL, entry.getKey()));
      }
    } else {
      context.getEventHandler().handle(
          new AMPSAgentEvent(AMPSAgentEventType.PSAGENT_KILL, event.getPsAgentId()));
    }
  }

  private void initPSAgents() throws InvalidParameterException {
    for (int i = 0; i < psAgentNumber; i++) {
      PSAgentId id = new PSAgentId(i);
      AMPSAgent agent = new AMPSAgent(context, id, new Location(ips[i], 0));
      psAgentMap.put(id, agent);
    }
  }

  @SuppressWarnings("unchecked")
  private void startPSAgens(PSAgentManagerEvent event) {
    if (event.getPsAgentId() == null) {
      for (Entry<PSAgentId, AMPSAgent> entry : psAgentMap.entrySet()) {
        context.getEventHandler().handle(
            new AMPSAgentEvent(AMPSAgentEventType.PSAGENT_SCHEDULE, entry.getKey()));
      }
    } else {
      context.getEventHandler().handle(
          new AMPSAgentEvent(AMPSAgentEventType.PSAGENT_SCHEDULE, event.getPsAgentId()));
    }
  }

  public int getPsAgentNumber() {
    return psAgentNumber;
  }

  public Resource getPsAgentResource() {
    return psAgentResource;
  }

  public Priority getPsAgentPriority() {
    return psAgentPriority;
  }

  public int getMaxAttemptNum() {
    return maxAttemptNum;
  }

  public AMPSAgent getPsAgent(PSAgentId psAgentId) {
    return psAgentMap.get(psAgentId);
  }

  public Map<PSAgentId, AMPSAgent> getPSAgentMap() {
    return psAgentMap;
  }

}
