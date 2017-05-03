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
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.psagent.PSAgentAttemptId;
import com.tencent.angel.psagent.PSAgentId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AMPSAgent implements EventHandler<AMPSAgentEvent> {
  private static final Log LOG = LogFactory.getLog(AMPSAgent.class);
  private final PSAgentId id;
  private final Location location;
  private final AMContext context;
  private PSAgentAttemptId runningAttemptId;
  private PSAgentAttemptId successAttemptId;
  private final List<String> diagnostics;
  private final List<PSAgentAttemptId> failedAttempts;
  private final Map<PSAgentAttemptId, PSAgentAttempt> attempts;
  private final StateMachine<AMPSAgentState, AMPSAgentEventType, AMPSAgentEvent> stateMachine;
  private final Lock readLock;
  private final Lock writeLock;
  private int nextAttemptNumber;

  protected static final StateMachineFactory<AMPSAgent, AMPSAgentState, AMPSAgentEventType, AMPSAgentEvent> stateMachineFactoryForPSMode =
      new StateMachineFactory<AMPSAgent, AMPSAgentState, AMPSAgentEventType, AMPSAgentEvent>(
          AMPSAgentState.NEW)

          // Transitions from the NEW state.
          .addTransition(AMPSAgentState.NEW, AMPSAgentState.NEW,
              EnumSet.of(AMPSAgentEventType.PSAGENT_SCHEDULE))
          .addTransition(AMPSAgentState.NEW, AMPSAgentState.KILLED,
              AMPSAgentEventType.PSAGENT_KILL, new KillNewTransition())
          .addTransition(AMPSAgentState.NEW, AMPSAgentState.FAILED,
              AMPSAgentEventType.PSAGENT_ERROR, new FailNewTransition())
          .addTransition(AMPSAgentState.NEW, AMPSAgentState.RUNNING,
              AMPSAgentEventType.PSAGENT_ATTEMPT_REGISTERED, new RegisteredTransition())

          // Transitions from the RUNNING state.
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.RUNNING,
              AMPSAgentEventType.PSAGENT_ATTEMPT_REGISTERED, new RegisteredTransition())
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.SUCCESS,
              AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS, new PSAgentDoneTransition())
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.KILLED,
              AMPSAgentEventType.PSAGENT_KILL, new KillTransition())
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.RUNNING,
              AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED, new AttemptKilledTransition())
          .addTransition(AMPSAgentState.RUNNING,
              EnumSet.of(AMPSAgentState.RUNNING, AMPSAgentState.FAILED),
              AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED, new AttemptFailedTransition())

          // Transitions from the SUCCEEDED state
          .addTransition(
              AMPSAgentState.SUCCESS,
              AMPSAgentState.SUCCESS,
              EnumSet.of(AMPSAgentEventType.PSAGENT_SCHEDULE,
                  AMPSAgentEventType.PSAGENT_CONTAINER_LAUNCH_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED,
                  AMPSAgentEventType.PSAGENT_DIAGNOSTICS_UPDATE,
                  AMPSAgentEventType.PSAGENT_UPDATE_STATE, AMPSAgentEventType.PSAGENT_ERROR,
                  AMPSAgentEventType.PSAGENT_KILL, AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_REGISTERED))

          // Transitions from the KILLED state
          .addTransition(
              AMPSAgentState.KILLED,
              AMPSAgentState.KILLED,
              EnumSet.of(AMPSAgentEventType.PSAGENT_SCHEDULE,
                  AMPSAgentEventType.PSAGENT_CONTAINER_LAUNCH_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED,
                  AMPSAgentEventType.PSAGENT_DIAGNOSTICS_UPDATE,
                  AMPSAgentEventType.PSAGENT_UPDATE_STATE, AMPSAgentEventType.PSAGENT_ERROR,
                  AMPSAgentEventType.PSAGENT_KILL, AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_REGISTERED))

          // Transitions from the FAILED state
          .addTransition(
              AMPSAgentState.FAILED,
              AMPSAgentState.FAILED,
              EnumSet.of(AMPSAgentEventType.PSAGENT_SCHEDULE,
                  AMPSAgentEventType.PSAGENT_CONTAINER_LAUNCH_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED,
                  AMPSAgentEventType.PSAGENT_DIAGNOSTICS_UPDATE,
                  AMPSAgentEventType.PSAGENT_UPDATE_STATE, AMPSAgentEventType.PSAGENT_ERROR,
                  AMPSAgentEventType.PSAGENT_KILL, AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_REGISTERED))

          .installTopology();

  protected static final StateMachineFactory<AMPSAgent, AMPSAgentState, AMPSAgentEventType, AMPSAgentEvent> stateMachineFactoryForAllMode =
      new StateMachineFactory<AMPSAgent, AMPSAgentState, AMPSAgentEventType, AMPSAgentEvent>(
          AMPSAgentState.NEW)

          // Transitions from the NEW state.
          .addTransition(AMPSAgentState.NEW, AMPSAgentState.SCHEDULED,
              AMPSAgentEventType.PSAGENT_SCHEDULE, new ScheduleTransition())

          .addTransition(AMPSAgentState.NEW, AMPSAgentState.KILLED,
              AMPSAgentEventType.PSAGENT_KILL, new KillNewTransition())

          // Transitions from the SCHEDULED state.
          .addTransition(AMPSAgentState.SCHEDULED, AMPSAgentState.RUNNING,
              AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED, new LaunchedTransition())

          .addTransition(AMPSAgentState.SCHEDULED, AMPSAgentState.KILLED,
              AMPSAgentEventType.PSAGENT_KILL, new KillTransition())
          .addTransition(AMPSAgentState.SCHEDULED,
              EnumSet.of(AMPSAgentState.SCHEDULED, AMPSAgentState.FAILED),
              AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED, new AttemptFailedTransition())
          .addTransition(AMPSAgentState.SCHEDULED, AMPSAgentState.SCHEDULED,
              AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED, new AttemptKilledTransition())

          // Transitions from the RUNNING state.
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.SUCCESS,
              AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS, new PSAgentDoneTransition())
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.KILLED,
              AMPSAgentEventType.PSAGENT_KILL, new KillTransition())
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.RUNNING,
              AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED, new AttemptKilledTransition())
          .addTransition(AMPSAgentState.RUNNING,
              EnumSet.of(AMPSAgentState.RUNNING, AMPSAgentState.FAILED),
              AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED, new AttemptFailedTransition())
          // another attempt launched,
          .addTransition(AMPSAgentState.RUNNING, AMPSAgentState.RUNNING,
              AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED)

          // Transitions from the SUCCEEDED state
          .addTransition(AMPSAgentState.SUCCESS, AMPSAgentState.SUCCESS,
              EnumSet.of(AMPSAgentEventType.PSAGENT_KILL))

          // Transitions from the KILLED state
          .addTransition(
              AMPSAgentState.KILLED,
              AMPSAgentState.KILLED,
              EnumSet.of(AMPSAgentEventType.PSAGENT_KILL,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS,
                  AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED))

          // Transitions from the FAILED state
          .addTransition(
              AMPSAgentState.FAILED,
              AMPSAgentState.FAILED,
              EnumSet
                  .of(AMPSAgentEventType.PSAGENT_KILL, AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED))

          .installTopology();

  public AMPSAgent(AMContext context, PSAgentId id, Location location) {
    this.context = context;
    this.id = id;
    this.location = location;
    this.diagnostics = new ArrayList<String>();
    this.failedAttempts = new ArrayList<PSAgentAttemptId>();
    this.attempts = new HashMap<PSAgentAttemptId, PSAgentAttempt>();
    if (context.getRunningMode() == RunningMode.ANGEL_PS_PSAGENT) {
      this.stateMachine = stateMachineFactoryForAllMode.make(this);
    } else {
      this.stateMachine = stateMachineFactoryForPSMode.make(this);
    }

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
  }

  public PSAgentId getId() {
    return id;
  }

  @Override
  public void handle(AMPSAgentEvent event) {
    LOG.debug("Processing " + event.getId() + " of type " + event.getType());
    writeLock.lock();
    try {
      final AMPSAgentState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {

      }
      if (oldState != getState()) {
        LOG.info(event.getId() + " AMPSAgent Transitioned from " + oldState + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public AMPSAgentState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private static class ScheduleTransition implements SingleArcTransition<AMPSAgent, AMPSAgentEvent> {
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      LOG.info("schedule psagent, ps agent id: " + psAgent.getId());
      psAgent.addAndScheduleAttempt();
    }
  }

  private static class FailNewTransition implements SingleArcTransition<AMPSAgent, AMPSAgentEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      psAgent.getContext().getEventHandler()
          .handle(new PSAgentManagerEvent(PSAgentManagerEventType.PSAGENT_FAILED, psAgent.getId()));
    }
  }

  private static class KillNewTransition implements SingleArcTransition<AMPSAgent, AMPSAgentEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      psAgent.getContext().getEventHandler()
          .handle(new PSAgentManagerEvent(PSAgentManagerEventType.PSAGENT_KILLED, psAgent.getId()));
    }
  }

  private static class LaunchedTransition implements SingleArcTransition<AMPSAgent, AMPSAgentEvent> {
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      psAgent.runningAttemptId = ((PSAgentFromAttemptEvent) event).getAttemptId();
    }
  }

  private static class RegisteredTransition implements
      SingleArcTransition<AMPSAgent, AMPSAgentEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      PSAgentFromAttemptEvent registerEvent = (PSAgentFromAttemptEvent) event;
      PSAgentAttempt attempt =
          new PSAgentAttempt(registerEvent.getAttemptId(), psAgent.context, psAgent.location);
      psAgent.attempts.put(attempt.getId(), attempt);

      LOG.info("create psagent attempt " + attempt.getId());
      psAgent
          .getContext()
          .getEventHandler()
          .handle(
              new PSAgentAttemptEvent(PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER, attempt
                  .getId()));
      psAgent.runningAttemptId = attempt.getId();
    }
  }

  private static class KillTransition implements SingleArcTransition<AMPSAgent, AMPSAgentEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      StringBuffer psdiaggostic = new StringBuffer();
      psdiaggostic.append("ps agent is killed by user, psId: ").append(psAgent.getId().toString());
      psAgent.diagnostics.add(psdiaggostic.toString());
      for (PSAgentAttempt attempt : psAgent.getAttempts().values()) {
        if (attempt != null && !attempt.isFinished()) {
          psAgent
              .getContext()
              .getEventHandler()
              .handle(
                  new PSAgentAttemptEvent(PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL, attempt
                      .getId()));
        }
      }
      psAgent.getContext().getEventHandler()
          .handle(new PSAgentManagerEvent(PSAgentManagerEventType.PSAGENT_KILLED, psAgent.getId()));
    }
  }

  private static class AttemptFailedTransition implements
      MultipleArcTransition<AMPSAgent, AMPSAgentEvent, AMPSAgentState> {
    @SuppressWarnings("unchecked")
    @Override
    public AMPSAgentState transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      PSAgentAttemptId attemptId = ((PSAgentFromAttemptEvent) event).getAttemptId();
      psAgent.failedAttempts.add(attemptId);
      if (psAgent.runningAttemptId == attemptId) {
        psAgent.runningAttemptId = null;
      }

      // add diagnostic
      StringBuffer psDiagnostic = new StringBuffer();
      psDiagnostic.append(attemptId.toString()).append(" failed due to: ")
          .append(psAgent.attempts.get(attemptId).getDiagnostics());
      if (LOG.isDebugEnabled()) {
        LOG.debug(attemptId + "failed due to:" + psDiagnostic.toString());
      }
      psAgent.diagnostics.add(psDiagnostic.toString());

      if (psAgent.failedAttempts.size() < psAgent.getContext().getPSAgentManager()
          .getMaxAttemptNum()) {
        psAgent.addAndScheduleAttempt();
        return psAgent.stateMachine.getCurrentState();
      } else {
        psAgent
            .getContext()
            .getEventHandler()
            .handle(
                new PSAgentManagerEvent(PSAgentManagerEventType.PSAGENT_FAILED, psAgent.getId()));
        return AMPSAgentState.FAILED;
      }
    }
  }

  private static class AttemptKilledTransition implements
      SingleArcTransition<AMPSAgent, AMPSAgentEvent> {

    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      PSAgentAttemptId attemptId = ((PSAgentFromAttemptEvent) event).getAttemptId();
      if (attemptId.equals(psAgent.runningAttemptId)) {
        psAgent.runningAttemptId = null;
      }
      psAgent.addAndScheduleAttempt();
    }
  }

  public static class PSAgentDoneTransition implements
      SingleArcTransition<AMPSAgent, AMPSAgentEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMPSAgent psAgent, AMPSAgentEvent event) {
      PSAgentAttemptId attemptId = ((PSAgentFromAttemptEvent) event).getAttemptId();
      psAgent.setSuccessAttemptId(attemptId);
      psAgent.getContext().getEventHandler()
          .handle(new PSAgentManagerEvent(PSAgentManagerEventType.PSAGENT_DONE, psAgent.getId()));
    }
  }

  @SuppressWarnings("unchecked")
  public void addAndScheduleAttempt() {
    PSAgentAttempt attempt =
        new PSAgentAttempt(new PSAgentAttemptId(id, nextAttemptNumber), context);
    attempts.put(attempt.getId(), attempt);

    LOG.info("scheduling " + attempt.getId());
    runningAttemptId = attempt.getId();
    nextAttemptNumber++;

    getContext().getEventHandler().handle(
        new PSAgentAttemptEvent(PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE, attempt.getId()));
  }

  public void setSuccessAttemptId(PSAgentAttemptId attemptId) {
    this.successAttemptId = attemptId;
  }

  public AMContext getContext() {
    return context;
  }

  public PSAgentAttemptId getRunningPSAttemptID() {
    return runningAttemptId;
  }

  public void setRunningPSAttemptID(PSAgentAttemptId runningAttemptId) {
    this.runningAttemptId = runningAttemptId;
  }

  public List<String> getDiagnostics() {
    return diagnostics;
  }

  public List<PSAgentAttemptId> getFailedAttempts() {
    return failedAttempts;
  }

  public Map<PSAgentAttemptId, PSAgentAttempt> getAttempts() {
    return attempts;
  }

  public PSAgentAttempt getAttempt(PSAgentAttemptId id) {
    return attempts.get(id);
  }

  public Location getLocation() {
    return location;
  }

}
