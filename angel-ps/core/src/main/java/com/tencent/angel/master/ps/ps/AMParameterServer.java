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

package com.tencent.angel.master.ps.ps;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.ps.ParameterServerManagerEvent;
import com.tencent.angel.master.ps.ParameterServerManagerEventType;
import com.tencent.angel.master.ps.attempt.PSAttempt;
import com.tencent.angel.master.ps.attempt.PSAttemptEvent;
import com.tencent.angel.master.ps.attempt.PSAttemptEventType;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.utils.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for a single ps, it manages all run attempts for the ps.
 * {@link com.tencent.angel.master.ps.attempt.PSAttempt} means a ps run attempt. Once the
 * running attempt for the ps is failed or killed, it will initialize and startup a new run attempt
 * for the ps if the number of failed/killed running attempts less than maximum number of failures
 * allowed
 */
public class AMParameterServer implements EventHandler<AMParameterServerEvent> {
  private static final Log LOG = LogFactory.getLog(AMParameterServer.class);
  
  /**ps id*/
  private final ParameterServerId id;
  private final AMContext context;
  private final Lock readLock;
  private final Lock writeLock;
  
  /**ps attempt id to ps attempt manager unit map*/
  private Map<PSAttemptId, PSAttempt> attempts;
  
  /**running ps attempt id*/
  private PSAttemptId runningPSAttemptId;
  
  /**failed ps attempt ids */
  private Set<PSAttemptId> failedAttempts;
  
  /**success ps attempt id*/
  private PSAttemptId successAttemptId;
  
  /**next ps attempt index*/
  private int nextAttemptNumber = 0;
  
  /**max attempt number for a ps*/
  private final int maxAttempts;
  
  /**ps diagnostics*/
  private final List<String> diagnostics = new ArrayList<String>();

  /**schedule timestamp of the ps*/
  //private long scheduledTime;
  
  /**expected machine ip which ps running on*/
  private final String ip;

  private static final PSDoneTransition PS_DONE_TRANSITION = new PSDoneTransition();

  protected static final StateMachineFactory<AMParameterServer, AMParameterServerState, AMParameterServerEventType, AMParameterServerEvent> stateMachineFactory =
      new StateMachineFactory<AMParameterServer, AMParameterServerState, AMParameterServerEventType, AMParameterServerEvent>(
          AMParameterServerState.NEW)

          // Transitions from the NEW state.
          .addTransition(AMParameterServerState.NEW, AMParameterServerState.SCHEDULED,
              AMParameterServerEventType.PS_SCHEDULE, new ScheduleTransition())

          .addTransition(AMParameterServerState.NEW, AMParameterServerState.KILLED,
              AMParameterServerEventType.PS_KILL, new KillNewTransition())

          // Transitions from the SCHEDULED state.
          .addTransition(AMParameterServerState.SCHEDULED, AMParameterServerState.RUNNING,
              AMParameterServerEventType.PS_ATTEMPT_LAUNCHED, new LaunchedTransition())

          .addTransition(AMParameterServerState.SCHEDULED, AMParameterServerState.KILLED,
              AMParameterServerEventType.PS_KILL, new KillTransition())
          .addTransition(AMParameterServerState.SCHEDULED,
              EnumSet.of(AMParameterServerState.SCHEDULED, AMParameterServerState.FAILED),
              AMParameterServerEventType.PS_ATTEMPT_FAILED, new AttemptFailedTransition())
          .addTransition(AMParameterServerState.SCHEDULED, 
              EnumSet.of(AMParameterServerState.SCHEDULED, AMParameterServerState.KILLED),
              AMParameterServerEventType.PS_ATTEMPT_KILLED, new AttemptKilledTransition())

          // Transitions from the RUNNING state.
          .addTransition(AMParameterServerState.RUNNING, AMParameterServerState.SUCCESS,
              AMParameterServerEventType.PS_ATTEMPT_SUCCESS, PS_DONE_TRANSITION)
          .addTransition(AMParameterServerState.RUNNING, AMParameterServerState.KILLED,
              AMParameterServerEventType.PS_KILL, new KillTransition())
          .addTransition(AMParameterServerState.RUNNING, 
              EnumSet.of(AMParameterServerState.RUNNING, AMParameterServerState.KILLED),
              AMParameterServerEventType.PS_ATTEMPT_KILLED, new AttemptKilledTransition())
          .addTransition(AMParameterServerState.RUNNING,
              EnumSet.of(AMParameterServerState.RUNNING, AMParameterServerState.FAILED),
              AMParameterServerEventType.PS_ATTEMPT_FAILED, new AttemptFailedTransition())
          // another attempt launched,
          .addTransition(AMParameterServerState.RUNNING, AMParameterServerState.RUNNING,
              AMParameterServerEventType.PS_ATTEMPT_LAUNCHED)

          // Transitions from the SUCCEEDED state
          .addTransition(AMParameterServerState.SUCCESS, AMParameterServerState.SUCCESS,
              EnumSet.of(AMParameterServerEventType.PS_KILL))

          // Transitions from the KILLED state
          .addTransition(
              AMParameterServerState.KILLED,
              AMParameterServerState.KILLED,
              EnumSet.of(AMParameterServerEventType.PS_KILL,
                  AMParameterServerEventType.PS_ATTEMPT_KILLED,
                  AMParameterServerEventType.PS_ATTEMPT_FAILED,
                  AMParameterServerEventType.PS_ATTEMPT_SUCCESS,
                  AMParameterServerEventType.PS_ATTEMPT_LAUNCHED))

          // Transitions from the FAILED state
          .addTransition(
              AMParameterServerState.FAILED,
              AMParameterServerState.FAILED,
              EnumSet.of(AMParameterServerEventType.PS_KILL,
                  AMParameterServerEventType.PS_ATTEMPT_KILLED))

          .installTopology();

  private final StateMachine<AMParameterServerState, AMParameterServerEventType, AMParameterServerEvent> stateMachine;

  public AMParameterServer(ParameterServerId id, AMContext context) {
    this(null, id, context);
  }

  public AMParameterServer(String ip, ParameterServerId id, AMContext context) {
    this.ip = ip;
    this.id = id;
    this.context = context;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    stateMachine = stateMachineFactory.make(this);
    attempts = new HashMap<PSAttemptId, PSAttempt>(2);
    this.failedAttempts = new HashSet<PSAttemptId>(2);
    maxAttempts =
        context.getConf().getInt(AngelConf.ANGEL_PS_MAX_ATTEMPTS,
            AngelConf.DEFAULT_PS_MAX_ATTEMPTS);
  }

  public void restart(PSLocation psLoc) {
    if(runningPSAttemptId != null && psLoc.loc.equals(context.getLocationManager().getPsLocation(psLoc.psId))) {
      getContext().getEventHandler().handle(
        new PSAttemptEvent(PSAttemptEventType.PA_FAILMSG, runningPSAttemptId));
    }
  }

  private static class ScheduleTransition implements
      SingleArcTransition<AMParameterServer, AMParameterServerEvent> {
    @Override
    public void transition(AMParameterServer parameterServer, AMParameterServerEvent event) {
      LOG.info("schedule ps server, psId: " + parameterServer.getId());
      parameterServer.addAndScheduleAttempt();
    }
  }

  private static class AttemptFailedTransition implements
      MultipleArcTransition<AMParameterServer, AMParameterServerEvent, AMParameterServerState> {
    @SuppressWarnings("unchecked")
    @Override
    public AMParameterServerState transition(AMParameterServer parameterServer,
        AMParameterServerEvent event) {
      PSAttemptId psAttemptId = ((PSPAttemptEvent) event).getPSAttemptId();
      parameterServer.failedAttempts.add(psAttemptId);
      if (parameterServer.runningPSAttemptId == psAttemptId) {
        parameterServer.runningPSAttemptId = null;
      }

      // add diagnostic
      StringBuilder psDiagnostic = new StringBuilder();
      psDiagnostic.append(psAttemptId.toString()).append(" failed due to: ")
          .append(StringUtils.join("\n", parameterServer.attempts.get(psAttemptId).getDiagnostics()));
      if (LOG.isDebugEnabled()) {
        LOG.debug(psAttemptId + "failed due to:" + psDiagnostic.toString());
      }
      parameterServer.diagnostics.add(psDiagnostic.toString());

      //check whether the number of failed attempts is less than the maximum number of allowed
      if (parameterServer.failedAttempts.size() < parameterServer.maxAttempts) {
        // Refresh ps location & matrix meta
        LOG.info("PS " + parameterServer.getId() + " failed, modify location and metadata");
        parameterServer.context.getLocationManager().setPsLocation(psAttemptId.getPsId(), null);
        if(parameterServer.context.getPSReplicationNum() > 1) {
          parameterServer.context.getMatrixMetaManager().psFailed(psAttemptId.getPsId());
        }

        //start a new attempt for this ps
        parameterServer.addAndScheduleAttempt();
        return parameterServer.stateMachine.getCurrentState();
      } else {
        //notify ps manager
        parameterServer
            .getContext()
            .getEventHandler()
            .handle(
                new ParameterServerManagerEvent(
                    ParameterServerManagerEventType.PARAMETERSERVER_FAILED, parameterServer.getId()));
        return AMParameterServerState.FAILED;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void addAndScheduleAttempt() {
    PSAttempt attempt = null;
    writeLock.lock();
    try {
      attempt = createPSAttempt();
      attempts.put(attempt.getId(), attempt);
      LOG.info("scheduling " + attempt.getId());
      runningPSAttemptId = attempt.getId();
    } finally {
      writeLock.unlock();
    }
    //getContext().getLocationManager().setPsLocation(id, null);
    getContext().getEventHandler().handle(
        new PSAttemptEvent(PSAttemptEventType.PA_SCHEDULE, attempt.getId()));
  }

  private PSAttempt createPSAttempt() {
    PSAttempt attempt = new PSAttempt(ip, id, nextAttemptNumber, context);
    nextAttemptNumber++;
    return attempt;
  }

  private static class LaunchedTransition implements
      SingleArcTransition<AMParameterServer, AMParameterServerEvent> {
    @Override
    public void transition(AMParameterServer parameterServer, AMParameterServerEvent event) {
      parameterServer.runningPSAttemptId = ((PSPAttemptEvent) event).getPSAttemptId();
    }
  }

  private static class KillNewTransition implements
      SingleArcTransition<AMParameterServer, AMParameterServerEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMParameterServer parameterServer, AMParameterServerEvent event) {
      parameterServer
          .getContext()
          .getEventHandler()
          .handle(
              new ParameterServerManagerEvent(ParameterServerManagerEventType.PARAMETERSERVER_KILLED,
                  parameterServer.getId()));
    }
  }

  private static class KillTransition implements
      SingleArcTransition<AMParameterServer, AMParameterServerEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMParameterServer parameterServer, AMParameterServerEvent event) {
      StringBuilder psdiaggostic = new StringBuilder();
      psdiaggostic.append("ps is killed by user, psId: ")
          .append(parameterServer.getId().toString());
      parameterServer.diagnostics.add(psdiaggostic.toString());
      for (PSAttempt attempt : parameterServer.attempts.values()) {
        if (attempt != null && !attempt.isFinished()) {
          parameterServer.getContext().getEventHandler()
              .handle(new PSAttemptEvent(PSAttemptEventType.PA_KILL, attempt.getId()));
        }
      }
      parameterServer
          .getContext()
          .getEventHandler()
          .handle(
              new ParameterServerManagerEvent(ParameterServerManagerEventType.PARAMETERSERVER_KILLED,
                  parameterServer.getId()));
    }
  }

  private static class AttemptKilledTransition implements
      MultipleArcTransition<AMParameterServer, AMParameterServerEvent, AMParameterServerState> {
    @SuppressWarnings("unchecked")
    @Override
    public AMParameterServerState transition(AMParameterServer parameterServer,
        AMParameterServerEvent event) {
      PSAttemptId psAttemptId = ((PSPAttemptEvent) event).getPSAttemptId();
      parameterServer.failedAttempts.add(psAttemptId);
      if (parameterServer.runningPSAttemptId == psAttemptId) {
        parameterServer.runningPSAttemptId = null;
      }

      // add diagnostic
      StringBuilder psDiagnostic = new StringBuilder();
      psDiagnostic.append(psAttemptId.toString()).append(" failed due to: ")
          .append(StringUtils.join("\n", parameterServer.attempts.get(psAttemptId).getDiagnostics()));
      if (LOG.isDebugEnabled()) {
        LOG.debug(psAttemptId + "failed due to:" + psDiagnostic.toString());
      }
      parameterServer.diagnostics.add(psDiagnostic.toString());

      // check whether the number of failed attempts is less than the maximum number of allowed
      if (parameterServer.failedAttempts.size() < parameterServer.maxAttempts) {
        // start a new attempt for this ps
        parameterServer.addAndScheduleAttempt();
        return parameterServer.stateMachine.getCurrentState();
      } else {
        // notify ps manager
        parameterServer
            .getContext()
            .getEventHandler()
            .handle(
                new ParameterServerManagerEvent(
                    ParameterServerManagerEventType.PARAMETERSERVER_KILLED, parameterServer.getId()));
        return AMParameterServerState.KILLED;
      }
    }
  }

  private static class PSDoneTransition implements
      SingleArcTransition<AMParameterServer, AMParameterServerEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMParameterServer parameterServer, AMParameterServerEvent event) {
      PSAttemptId attemptId = ((PSPAttemptEvent) event).getPSAttemptId();
      parameterServer.successAttemptId = attemptId;
      parameterServer.runningPSAttemptId = null;
      parameterServer
          .getContext()
          .getEventHandler()
          .handle(
              new ParameterServerManagerEvent(ParameterServerManagerEventType.PARAMETERSERVER_DONE,
                  parameterServer.getId()));
    }
  }

  @Override
  public void handle(AMParameterServerEvent event) {
    LOG.debug("Processing " + event.getPsId() + " of type " + event.getType());
    writeLock.lock();
    try {
      final AMParameterServerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {

      }
      if (oldState != getState()) {
        LOG.info(event.getPsId() + " AMParameterServer Transitioned from " + oldState
            + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * get ps state
   * @return AMParameterServerState ps state
   */
  public AMParameterServerState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  public AMContext getContext() {
    return context;
  }

  /**
   * get ps id
   * @return ParameterServerId ps id
   */
  public ParameterServerId getId() {
    return id;
  }

  /**
   * get ps diagnostics
   * @return List<String> ps diagnostics
   */
  public List<String> getDiagnostics() {
    try{
      readLock.lock();
      List<String> cloneDiagnostics = new ArrayList<String>(diagnostics.size());
      cloneDiagnostics.addAll(diagnostics);
      return cloneDiagnostics;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get ps attempt use attempt id
   * @param psAttemptId ps attempt id
   * @return PSAttempt ps attempt
   */
  public PSAttempt getPSAttempt(PSAttemptId psAttemptId) {
    readLock.lock();
    PSAttempt psAttempt = null;
    try {
      psAttempt = attempts.get(psAttemptId);
    } finally {
      readLock.unlock();
    }
    return psAttempt;
  }

  /**
   * get ps attempt id to ps attempt map
   * @return Map<PSAttemptId, PSAttempt> ps attempt id to ps attempt map
   */
  public Map<PSAttemptId, PSAttempt> getPSAttempts() {
    Map<PSAttemptId, PSAttempt> ret = new HashMap<PSAttemptId, PSAttempt>();
    readLock.lock();
    try {
      for (Entry<PSAttemptId, PSAttempt> attempt : attempts.entrySet()) {
        ret.put(attempt.getKey(), attempt.getValue());
      }
    } finally {
      readLock.unlock();
    }

    return ret;
  }

  /**
   * get next attempt index
   * @return int next attempt index
   */
  public int getNextAttemptNumber() {
    try{
      readLock.lock();
      return nextAttemptNumber;
    } finally {
      readLock.unlock();
    }    
  }

  /**
   * set next attempt index
   * @param nextAttemptNumber next attempt index
   */
  public void setNextAttemptNumber(int nextAttemptNumber) {
    try{
      writeLock.lock();
      this.nextAttemptNumber = nextAttemptNumber;
    } finally {
      writeLock.unlock();
    }    
  }

  /**
   * get id of success ps attempt
   * @return PSAttemptId id of success ps attempt
   */
  public PSAttemptId getSuccessAttemptId() {
    try{
      readLock.lock();
      return successAttemptId;
    } finally {
      readLock.unlock();
    }     
  }

  /**
   * get id of running ps attempt
   * @return PSAttemptId id of running ps attempt
   */
  public PSAttemptId getRunningAttemptId() {
    try{
      readLock.lock();
      return runningPSAttemptId;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get max attempt number
   * @return int max attempt number
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }
}
