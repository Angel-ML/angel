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

package com.tencent.angel.master.ps.attempt;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.deploy.ContainerAllocatorEvent;
import com.tencent.angel.master.deploy.ContainerAllocatorEventType;
import com.tencent.angel.master.deploy.ContainerLauncherEvent;
import com.tencent.angel.master.deploy.ContainerLauncherEventType;
import com.tencent.angel.master.deploy.local.LocalContainerAllocatorEvent;
import com.tencent.angel.master.deploy.local.LocalContainerLauncherEvent;
import com.tencent.angel.master.deploy.yarn.ContainerRemoteLaunchEvent;
import com.tencent.angel.master.deploy.yarn.YarnContainerAllocatorEvent;
import com.tencent.angel.master.deploy.yarn.YarnContainerLauncherEvent;
import com.tencent.angel.master.deploy.yarn.YarnContainerRequestEvent;
import com.tencent.angel.master.ps.ps.AMParameterServerEventType;
import com.tencent.angel.master.ps.ps.PSPAttemptEvent;
import com.tencent.angel.master.yarn.util.ContainerContextUtils;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.ParameterServerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A running attempt for the ps.
 */
public class PSAttempt implements EventHandler<PSAttemptEvent> {

  private static final Log LOG = LogFactory.getLog(PSAttempt.class);
  /** ps attempt id */
  private final PSAttemptId attemptId;

  /** ps attempt diagnostics */
  private final List<String> diagnostics;

  private final Lock readLock;
  private final Lock writeLock;
  private final AMContext context;

  /** ps attempt start time */
  private long launchTime;

  /** ps attempt finish time */
  private long finishTime;

  /** ps attempt running address(ip and port) */
  private Location location;

  /** ps attempt metrices */
  private final Map<String, String> metrices;

  /** except machine to run this ps attempt */
  private final String expectedIp;

  /** container allocated for this ps attempt */
  private Container container;

  private static final DiagnosticUpdaterTransition DIAGNOSTIC_UPDATE_TRANSITION =
      new DiagnosticUpdaterTransition();

  private static final StateMachineFactory<PSAttempt, PSAttemptStateInternal, PSAttemptEventType, PSAttemptEvent> stateMachineFactory =
      new StateMachineFactory<PSAttempt, PSAttemptStateInternal, PSAttemptEventType, PSAttemptEvent>(
          PSAttemptStateInternal.NEW)

          // Transitions from the NEW state.
          .addTransition(PSAttemptStateInternal.NEW, PSAttemptStateInternal.SCHEDULED,
              PSAttemptEventType.PA_SCHEDULE, new RequestContainerTransition())

          .addTransition(PSAttemptStateInternal.NEW, PSAttemptStateInternal.KILLED,
              PSAttemptEventType.PA_KILL,
              new PSAttemptFinishedTransition(PSAttemptStateInternal.KILLED))

          .addTransition(PSAttemptStateInternal.NEW, PSAttemptStateInternal.FAILED,
              PSAttemptEventType.PA_FAILMSG,
              new PSAttemptFinishedTransition(PSAttemptStateInternal.FAILED))
          .addTransition(PSAttemptStateInternal.NEW, PSAttemptStateInternal.NEW,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the UNASSIGNED state.
          .addTransition(PSAttemptStateInternal.SCHEDULED, PSAttemptStateInternal.ASSIGNED,
              PSAttemptEventType.PA_CONTAINER_ASSIGNED, new ContainerAssignedTransition())
          .addTransition(PSAttemptStateInternal.SCHEDULED, PSAttemptStateInternal.KILLED,
              PSAttemptEventType.PA_KILL,
              new PSAttemptFinishedTransition(PSAttemptStateInternal.KILLED))
          // when user kill task, or task timeout, psAttempt will receive TA_FAILMSG event
          .addTransition(PSAttemptStateInternal.SCHEDULED, PSAttemptStateInternal.FAILED,
              PSAttemptEventType.PA_FAILMSG, new DeallocateContainerTransition())
          .addTransition(PSAttemptStateInternal.SCHEDULED, PSAttemptStateInternal.SCHEDULED,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the ASSIGNED state.
          .addTransition(PSAttemptStateInternal.ASSIGNED, PSAttemptStateInternal.RUNNING,
              PSAttemptEventType.PA_CONTAINER_LAUNCHED, new LaunchedContainerTransition())
          .addTransition(
              PSAttemptStateInternal.ASSIGNED,
              PSAttemptStateInternal.FAILED,
              EnumSet.of(PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED,
                  PSAttemptEventType.PA_FAILMSG),
              new PSAttemptFinishedTransition(PSAttemptStateInternal.FAILED))
          .addTransition(PSAttemptStateInternal.ASSIGNED, PSAttemptStateInternal.KILLED,
              PSAttemptEventType.PA_KILL,
              new PSAttemptFinishedTransition(PSAttemptStateInternal.KILLED))
          .addTransition(PSAttemptStateInternal.ASSIGNED, PSAttemptStateInternal.ASSIGNED,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)
          // this happened when launch thread run slowly, and PA_REGISTER event dispatched before
          // PA_CONTAINER_LAUNCHED event
          .addTransition(PSAttemptStateInternal.ASSIGNED, PSAttemptStateInternal.ASSIGNED,
              PSAttemptEventType.PA_REGISTER, new RegisterTransition())

          // Transitions from the PSAttemptStateInternal.RUNNING state.
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.RUNNING,
              PSAttemptEventType.PA_UPDATE_STATE, new StateUpdateTransition())
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.RUNNING,
              PSAttemptEventType.PA_REGISTER, new RegisterTransition())
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.COMMITTING,
              PSAttemptEventType.PA_COMMIT)
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.FAILED,
              EnumSet.of(PSAttemptEventType.PA_CONTAINER_COMPLETE, PSAttemptEventType.PA_FAILMSG),
              new PSAttemptFinishedTransition(PSAttemptStateInternal.FAILED))
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.KILLED,
              PSAttemptEventType.PA_KILL,
              new PSAttemptFinishedTransition(PSAttemptStateInternal.KILLED))
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.RUNNING,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(PSAttemptStateInternal.RUNNING, PSAttemptStateInternal.SUCCESS,
              PSAttemptEventType.PA_SUCCESS,
              new PSAttemptFinishedTransition(PSAttemptStateInternal.SUCCESS))

          // Transitions from the PSAttemptStateInternal.COMMITTING state
          /*
           * .addTransition(PSAttemptStateInternal.COMMITTING, PSAttemptStateInternal.SUCCESS,
           * PSAttemptEventType.PA_SUCCESS, new
           * PSAttemptFinishedTransition(PSAttemptStateInternal.SUCCESS))
           * .addTransition(PSAttemptStateInternal.COMMITTING, PSAttemptStateInternal.FAILED,
           * EnumSet.of( PSAttemptEventType.PA_CONTAINER_COMPLETE, PSAttemptEventType.PA_FAILMSG),
           * new PSAttemptFinishedTransition(PSAttemptStateInternal.FAILED))
           * .addTransition(PSAttemptStateInternal.COMMITTING, PSAttemptStateInternal.KILLED,
           * PSAttemptEventType.PA_KILL, new
           * PSAttemptFinishedTransition(PSAttemptStateInternal.KILLED))
           * .addTransition(PSAttemptStateInternal.COMMITTING, PSAttemptStateInternal.COMMITTING,
           * PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)
           */

          // Transitions from the PSAttemptStateInternal.KILLED state
          .addTransition(
              PSAttemptStateInternal.KILLED,
              PSAttemptStateInternal.KILLED,
              EnumSet.of(PSAttemptEventType.PA_COMMIT, PSAttemptEventType.PA_SUCCESS,
                  PSAttemptEventType.PA_CONTAINER_COMPLETE,
                  PSAttemptEventType.PA_CONTAINER_ASSIGNED,
                  PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED,
                  PSAttemptEventType.PA_CONTAINER_LAUNCHED, PSAttemptEventType.PA_KILL,
                  PSAttemptEventType.PA_REGISTER, PSAttemptEventType.PA_SCHEDULE,
                  PSAttemptEventType.PA_FAILMSG))
          .addTransition(PSAttemptStateInternal.KILLED, PSAttemptStateInternal.KILLED,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the PSAttemptStateInternal.FAILED state
          .addTransition(
              PSAttemptStateInternal.FAILED,
              PSAttemptStateInternal.FAILED,
              EnumSet.of(PSAttemptEventType.PA_COMMIT, PSAttemptEventType.PA_SUCCESS,
                  PSAttemptEventType.PA_CONTAINER_COMPLETE,
                  PSAttemptEventType.PA_CONTAINER_ASSIGNED,
                  PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED,
                  PSAttemptEventType.PA_CONTAINER_LAUNCHED, PSAttemptEventType.PA_KILL,
                  PSAttemptEventType.PA_REGISTER, PSAttemptEventType.PA_SCHEDULE,
                  PSAttemptEventType.PA_FAILMSG))
          .addTransition(PSAttemptStateInternal.FAILED, PSAttemptStateInternal.FAILED,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)


          .addTransition(PSAttemptStateInternal.KILLED, PSAttemptStateInternal.KILLED,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              PSAttemptStateInternal.SUCCESS,
              PSAttemptStateInternal.SUCCESS,
              EnumSet.of(PSAttemptEventType.PA_COMMIT, PSAttemptEventType.PA_SUCCESS,
                  PSAttemptEventType.PA_CONTAINER_COMPLETE,
                  PSAttemptEventType.PA_CONTAINER_ASSIGNED,
                  PSAttemptEventType.PA_CONTAINER_LAUNCH_FAILED,
                  PSAttemptEventType.PA_CONTAINER_LAUNCHED, PSAttemptEventType.PA_KILL,
                  PSAttemptEventType.PA_REGISTER, PSAttemptEventType.PA_SCHEDULE,
                  PSAttemptEventType.PA_FAILMSG))
          .addTransition(PSAttemptStateInternal.SUCCESS, PSAttemptStateInternal.SUCCESS,
              PSAttemptEventType.PA_DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          // create the topology tables
          .installTopology();

  private final StateMachine<PSAttemptStateInternal, PSAttemptEventType, PSAttemptEvent> stateMachine;

  /**
   * Init the Attempt for PS
   * @param psId ps id
   * @param attemptIndex attempt index
   * @param amContext Master context
   */
  public PSAttempt(ParameterServerId psId, int attemptIndex, AMContext amContext) {
    this(null, psId, attemptIndex, amContext);
  }

  /**
   * Init the Attempt for PS
   * @param ip excepted host for this ps attempt
   * @param psId ps id
   * @param attemptIndex attempt index
   * @param amContext Master context
   */
  public PSAttempt(String ip, ParameterServerId psId, int attemptIndex, AMContext amContext) {
    this.expectedIp = ip;
    attemptId = new PSAttemptId(psId, attemptIndex);
    this.context = amContext;
    stateMachine = stateMachineFactory.make(this);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    metrices = new HashMap<String, String>();
    diagnostics = new ArrayList<String>();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void handle(PSAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getPSAttemptId() + " of type " + event.getType());
    }
    writeLock.lock();
    try {
      final PSAttemptStateInternal oldState = getInternalState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for " + this.attemptId, e);
        context.getEventHandler()
            .handle(
                new InternalErrorEvent(context.getApplicationId(), "Invalid event :"
                    + event.getType()));
      }
      if (oldState != getInternalState()) {
        LOG.info(attemptId + " PSAttempt Transitioned from " + oldState + " to "
            + getInternalState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  static class RequestContainerTransition implements SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent event) {
      LOG.info("allocate ps server attempt resource, ps attempt id = " + psAttempt.getId());

      // reqeuest resource:send a resource request to the resource allocator
      AngelDeployMode deployMode = psAttempt.getContext().getDeployMode();
      ContainerAllocatorEvent allocatorEvent = null;

      if (deployMode == AngelDeployMode.LOCAL) {
        allocatorEvent =
            new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ,
                psAttempt.getId());

      } else {
        if (psAttempt.getExpectedIp() != null) {
          allocatorEvent =
              new YarnContainerRequestEvent(psAttempt.getId(), psAttempt.getContext()
                  .getParameterServerManager().getPsResource(), psAttempt.getContext()
                  .getParameterServerManager().getPriority(),
                  new String[] {psAttempt.getExpectedIp()});
        } else {
          allocatorEvent =
              new YarnContainerRequestEvent(psAttempt.getId(), psAttempt.getContext()
                  .getParameterServerManager().getPsResource(), psAttempt.getContext()
                  .getParameterServerManager().getPriority());
        }
      }

      psAttempt.getContext().getEventHandler().handle(allocatorEvent);
    }
  }

  private static class ContainerAssignedTransition implements
      SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @SuppressWarnings({"unchecked"})
    @Override
    public void transition(final PSAttempt psAttempt, PSAttemptEvent event) {
      PSAttemptContainerAssignedEvent assignedEvent = (PSAttemptContainerAssignedEvent) event;
      PSAttemptId psAttemptId = psAttempt.getId();
      psAttempt.container = assignedEvent.getContainer();

      // Once the resource is applied, build and send the launch request to the container launcher
      AngelDeployMode deployMode = psAttempt.getContext().getDeployMode();
      ContainerLauncherEvent launchEvent = null;
      if (deployMode == AngelDeployMode.LOCAL) {
        launchEvent =
            new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH,
                psAttempt.getId());
      } else {
        ContainerLaunchContext launchContext =
            ContainerContextUtils.createContainerLaunchContext(psAttempt.getContext()
                .getContainerAllocator().getApplicationACLs(), psAttempt.getContext().getConf(),
                psAttemptId, psAttempt.getContext().getApplicationId(), psAttempt.getContext()
                    .getMasterService(), psAttempt.getContext().getCredentials());

        launchEvent =
            new ContainerRemoteLaunchEvent(psAttemptId, launchContext, assignedEvent.getContainer());
      }

      psAttempt.getContext().getEventHandler().handle(launchEvent);
    }
  }

  private static class DeallocateContainerTransition implements
      SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent event) {
      psAttempt.setFinishTime();

      // deallocator the resource of the ps attempt:send a resource deallocator request to the
      // resource allocator
      AngelDeployMode deployMode = psAttempt.getContext().getDeployMode();
      ContainerAllocatorEvent allocatorEvent = null;
      if (deployMode == AngelDeployMode.LOCAL) {
        allocatorEvent =
            new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_DEALLOCATE,
                psAttempt.getId());

      } else {
        allocatorEvent =
            new YarnContainerAllocatorEvent(psAttempt.getId(),
                ContainerAllocatorEventType.CONTAINER_DEALLOCATE, psAttempt.context
                    .getParameterServerManager().getPriority());
      }
      psAttempt.getContext().getEventHandler().handle(allocatorEvent);
    }
  }

  private static class StateUpdateTransition implements
      SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent event) {
      PSAttemptStateUpdateEvent updateEvent = (PSAttemptStateUpdateEvent) event;
      Map<String, String> params = updateEvent.getParams();
      psAttempt.metrices.putAll(params);
    }
  }

  private static class LaunchedContainerTransition implements
      SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent evnt) {
      // set the launch time
      psAttempt.launchTime = psAttempt.getContext().getClock().getTime();

      psAttempt
          .getContext()
          .getEventHandler()
          .handle(
              new PSPAttemptEvent(psAttempt.attemptId,
                  AMParameterServerEventType.PS_ATTEMPT_LAUNCHED));

      // add the ps attempt to the heartbeat timeout monitoring list
      psAttempt.getContext().getParameterServerManager().register(psAttempt.attemptId);
      LOG.info("has telled attempt started for attempid: " + psAttempt.attemptId);
    }
  }

  private static class RegisterTransition implements SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent event) {
      // parse ps attempt location and put it to location manager
      PSAttemptRegisterEvent registerEvent = (PSAttemptRegisterEvent) event;
      psAttempt.location = registerEvent.getLocation();
      LOG.info(psAttempt.attemptId + " is registering, location: " + psAttempt.location);
      psAttempt.getContext().getLocationManager()
          .setPsLocation(psAttempt.attemptId.getPsId(), psAttempt.location);
    }
  }

  private static class PSAttemptFinishedTransition implements
      SingleArcTransition<PSAttempt, PSAttemptEvent> {
    private final PSAttemptStateInternal finishState;

    PSAttemptFinishedTransition(PSAttemptStateInternal finishState) {
      this.finishState = finishState;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent event) {
      psAttempt.setFinishTime();
      // send PS_ATTEMPT_FAILED to AMParameterServer, AMParameterServer will retry another attempt
      // or failed
      switch (finishState) {
        case FAILED:
          psAttempt
              .getContext()
              .getEventHandler()
              .handle(
                  new PSPAttemptEvent(psAttempt.attemptId,
                      AMParameterServerEventType.PS_ATTEMPT_FAILED));
          break;
        case KILLED:
          psAttempt
              .getContext()
              .getEventHandler()
              .handle(
                  new PSPAttemptEvent(psAttempt.attemptId,
                      AMParameterServerEventType.PS_ATTEMPT_KILLED));
          break;
        case SUCCESS:
          psAttempt
              .getContext()
              .getEventHandler()
              .handle(
                  new PSPAttemptEvent(psAttempt.attemptId,
                      AMParameterServerEventType.PS_ATTEMPT_SUCCESS));
          break;
        default:
          LOG.error("invalid PSAttemptStateInternal in PSAttemptFinishedTransition!");
          break;
      }

      // remove ps attempt id from heartbeat timeout monitor list
      psAttempt.getContext().getParameterServerManager().unRegister(psAttempt.attemptId);

      // release container:send a release request to container launcher
      AngelDeployMode deployMode = psAttempt.getContext().getDeployMode();
      ContainerLauncherEvent launchEvent = null;
      if (deployMode == AngelDeployMode.LOCAL) {
        launchEvent =
            new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP,
                psAttempt.attemptId);
      } else {
        launchEvent =
            new YarnContainerLauncherEvent(psAttempt.getId(), psAttempt.container.getId(),
                StringInterner.weakIntern(psAttempt.container.getNodeId().toString()),
                psAttempt.container.getContainerToken(),
                ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP);
      }
      psAttempt.getContext().getEventHandler().handle(launchEvent);
    }
  }

  private static class DiagnosticUpdaterTransition implements
      SingleArcTransition<PSAttempt, PSAttemptEvent> {
    @Override
    public void transition(PSAttempt psAttempt, PSAttemptEvent event) {
      PSAttemptDiagnosticsUpdateEvent diagEvent = (PSAttemptDiagnosticsUpdateEvent) event;
      LOG.info("Diagnostics report from " + psAttempt.getId() + ": " + diagEvent.getDiagnostics());
      psAttempt.addDiagnostic(diagEvent.getDiagnostics());
    }
  }

  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = context.getClock().getTime();
    }
  }

  private void addDiagnostic(String diag) {
    if (diag != null && !diag.equals("")) {
      diagnostics.add(diag);
    }
  }

  /**
   * get ps attempt state
   * 
   * @return PSAttemptStateInternal ps attempt state
   */
  public PSAttemptStateInternal getInternalState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the http address of the machine where the container is located
   * 
   * @return String the http address of the machine where the container is located
   */
  public String getNodeHttpAddr() {
    try {
      readLock.lock();
      if (container == null) {
        return null;
      } else {
        return container.getNodeHttpAddress();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get container id string
   * 
   * @return String container id string
   */
  public String getContainerIdStr() {
    try {
      readLock.lock();
      if (container == null) {
        return null;
      } else {
        return container.getId().toString();
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the container allocated to this ps attempt
   * 
   * @return Container the container allocated to this ps attempt
   */
  public Container getContainer() {
    try {
      readLock.lock();
      return container;
    } finally {
      readLock.unlock();
    }
  }

  public String getExpectedIp() {
    return expectedIp;
  }

  /**
   * get ps attempt location
   * 
   * @return Location ps attempt location
   */
  public Location getLocation() {
    try {
      readLock.lock();
      return location;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get ps attempt metrices
   * 
   * @return Map<String, String> ps attempt metrices
   */
  public Map<String, String> getMetrices() {
    try {
      readLock.lock();
      Map<String, String> cloneMetrices = new HashMap<String, String>();
      cloneMetrices.putAll(metrices);
      return cloneMetrices;
    } finally {
      readLock.unlock();
    }
  }

  public AMContext getContext() {
    return context;
  }

  /**
   * get ps attempt launch time
   * 
   * @return long ps attempt launch time
   */
  public long getLaunchTime() {
    try {
      readLock.lock();
      return launchTime;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get ps attempt finish time
   * 
   * @return long ps attempt finish time
   */
  public long getFinishTime() {
    try {
      readLock.lock();
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get ps attempt id
   * 
   * @return ps attempt id
   */
  public PSAttemptId getId() {
    return attemptId;
  }

  /**
   * check if the ps attempt finish or not
   * 
   * @return boolean
   */
  public boolean isFinished() {
    PSAttemptStateInternal state = getInternalState();
    return (state == PSAttemptStateInternal.SUCCESS || state == PSAttemptStateInternal.FAILED || state == PSAttemptStateInternal.KILLED);
  }

  /**
   * get ps attempt diagnostices
   * 
   * @return List<String> ps attempt diagnostices
   */
  public List<String> getDiagnostics() {
    try {
      readLock.lock();
      List<String> cloneDiagnostics = new ArrayList<String>(diagnostics.size());
      cloneDiagnostics.addAll(diagnostics);
      return cloneDiagnostics;
    } finally {
      readLock.unlock();
    }
  }
}
