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

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.RunningMode;
import com.tencent.angel.common.AngelEnvironment;
import com.tencent.angel.common.Location;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.master.MasterService;
import com.tencent.angel.master.app.AMContext;
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
import com.tencent.angel.master.yarn.util.AngelApps;
import com.tencent.angel.psagent.PSAgentAttemptId;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PSAgentAttempt implements EventHandler<PSAgentAttemptEvent> {
  private static final Log LOG = LogFactory.getLog(PSAgentAttempt.class);
  private Container container;
  private final StateMachine<PSAgentAttemptState, PSAgentAttemptEventType, PSAgentAttemptEvent> stateMachine;
  private long launchTime;
  private String trackerName;
  private int httpPort;
  private Location location;
  private final PSAgentAttemptId id;
  private final AMContext context;
  private final Lock readLock;
  private final Lock writeLock;
  private String diagnostics;
  private long finishTime;

  private static AtomicBoolean initialClasspathFlag = new AtomicBoolean();
  private static String initialClasspath = null;
  private static String initialAppClasspath = null;
  private static final Object commonContainerSpecLock = new Object();
  private static ContainerLaunchContext commonContainerSpec = null;
  private static final Object classpathLock = new Object();

  public PSAgentAttempt(PSAgentAttemptId attemptId, AMContext context) {
    this(attemptId, context, null);
  }

  public PSAgentAttempt(PSAgentAttemptId attemptId, AMContext context, Location location) {
    this.id = attemptId;
    this.context = context;
    this.location = location;
    if (context.getRunningMode() == RunningMode.ANGEL_PS_PSAGENT) {
      stateMachine = stateMachineFactoryForAllMode.make(this);
    } else {
      stateMachine = stateMachineFactoryForPSMode.make(this);
    }

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  private static final DiagnosticUpdaterTransition DIAGNOSTIC_UPDATE_TRANSITION =
      new DiagnosticUpdaterTransition();

  private static final StateMachineFactory<PSAgentAttempt, PSAgentAttemptState, PSAgentAttemptEventType, PSAgentAttemptEvent> stateMachineFactoryForPSMode =
      new StateMachineFactory<PSAgentAttempt, PSAgentAttemptState, PSAgentAttemptEventType, PSAgentAttemptEvent>(
          PSAgentAttemptState.NEW)

          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
              new FinishedTransition(PSAgentAttemptState.KILLED))
          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.FAILED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG,
              new FinishedTransition(PSAgentAttemptState.FAILED))
          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.NEW,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER, new RegisterForPSModeTransition())

          // Transitions from the PSAttemptStateInternal.RUNNING state.
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_UPDATE_STATE, new StateUpdateTransition())
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER, new RegisterForPSModeTransition())
          .addTransition(
              PSAgentAttemptState.RUNNING,
              PSAgentAttemptState.FAILED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG),
              new FinishedTransition(PSAgentAttemptState.FAILED))
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
              new FinishedTransition(PSAgentAttemptState.KILLED))
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.SUCCESS,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
              new FinishedTransition(PSAgentAttemptState.SUCCESS))

          // Transitions from the PSAttemptStateInternal.KILLED state
          .addTransition(
              PSAgentAttemptState.KILLED,
              PSAgentAttemptState.KILLED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_COMMIT,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG))
          .addTransition(PSAgentAttemptState.KILLED, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the PSAttemptStateInternal.FAILED state
          .addTransition(
              PSAgentAttemptState.FAILED,
              PSAgentAttemptState.FAILED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_COMMIT,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG))
          .addTransition(PSAgentAttemptState.FAILED, PSAgentAttemptState.FAILED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)


          .addTransition(
              PSAgentAttemptState.SUCCESS,
              PSAgentAttemptState.SUCCESS,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_COMMIT,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG))
          .addTransition(PSAgentAttemptState.SUCCESS, PSAgentAttemptState.SUCCESS,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          // create the topology tables
          .installTopology();

  private static final StateMachineFactory<PSAgentAttempt, PSAgentAttemptState, PSAgentAttemptEventType, PSAgentAttemptEvent> stateMachineFactoryForAllMode =
      new StateMachineFactory<PSAgentAttempt, PSAgentAttemptState, PSAgentAttemptEventType, PSAgentAttemptEvent>(
          PSAgentAttemptState.NEW)

          // Transitions from the NEW state.
          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.SCHEDULED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE, new RequestContainerTransition())

          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
              new FinishedTransition(PSAgentAttemptState.KILLED))
          // PA_FAILMSG

          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.FAILED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG,
              new FinishedTransition(PSAgentAttemptState.FAILED))
          .addTransition(PSAgentAttemptState.NEW, PSAgentAttemptState.NEW,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the UNASSIGNED state.
          .addTransition(PSAgentAttemptState.SCHEDULED, PSAgentAttemptState.ASSIGNED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
              new ContainerAssignedTransition())
          .addTransition(PSAgentAttemptState.SCHEDULED, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
              new FinishedTransition(PSAgentAttemptState.KILLED))
          // when user kill task, or task timeout, psAttempt will receive TA_FAILMSG
          // event
          .addTransition(PSAgentAttemptState.SCHEDULED, PSAgentAttemptState.FAILED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG, new DeallocateContainerTransition())
          .addTransition(PSAgentAttemptState.SCHEDULED, PSAgentAttemptState.SCHEDULED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the ASSIGNED state.
          .addTransition(PSAgentAttemptState.ASSIGNED, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
              new LaunchedContainerTransition())
          .addTransition(
              PSAgentAttemptState.ASSIGNED,
              PSAgentAttemptState.FAILED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG),
              new FinishedTransition(PSAgentAttemptState.FAILED))
          .addTransition(PSAgentAttemptState.ASSIGNED, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
              new FinishedTransition(PSAgentAttemptState.KILLED))
          .addTransition(PSAgentAttemptState.ASSIGNED, PSAgentAttemptState.ASSIGNED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          // this happened when launch thread run slowly, and PA_REGISTER event
          // dispatched before PA_CONTAINER_LAUNCHED event
          .addTransition(PSAgentAttemptState.ASSIGNED, PSAgentAttemptState.ASSIGNED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER, new RegisterTransition())

          // Transitions from the PSAttemptStateInternal.RUNNING state.
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_UPDATE_STATE, new StateUpdateTransition())
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER, new RegisterTransition())
          .addTransition(
              PSAgentAttemptState.RUNNING,
              PSAgentAttemptState.FAILED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG),
              new FinishedTransition(PSAgentAttemptState.FAILED))
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
              new FinishedTransition(PSAgentAttemptState.KILLED))
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.RUNNING,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)
          .addTransition(PSAgentAttemptState.RUNNING, PSAgentAttemptState.SUCCESS,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
              new FinishedTransition(PSAgentAttemptState.SUCCESS))

          // Transitions from the PSAttemptStateInternal.KILLED state
          .addTransition(
              PSAgentAttemptState.KILLED,
              PSAgentAttemptState.KILLED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_COMMIT,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG))
          .addTransition(PSAgentAttemptState.KILLED, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          // Transitions from the PSAttemptStateInternal.FAILED state
          .addTransition(
              PSAgentAttemptState.FAILED,
              PSAgentAttemptState.FAILED,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_COMMIT,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG))
          .addTransition(PSAgentAttemptState.FAILED, PSAgentAttemptState.FAILED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(PSAgentAttemptState.KILLED, PSAgentAttemptState.KILLED,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              PSAgentAttemptState.SUCCESS,
              PSAgentAttemptState.SUCCESS,
              EnumSet.of(PSAgentAttemptEventType.PSAGENT_ATTEMPT_COMMIT,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SUCCESS,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_COMPLETE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_ASSIGNED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_CONTAINER_LAUNCHED,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_KILL,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_REGISTER,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_SCHEDULE,
                  PSAgentAttemptEventType.PSAGENT_ATTEMPT_FAILMSG))
          .addTransition(PSAgentAttemptState.SUCCESS, PSAgentAttemptState.SUCCESS,
              PSAgentAttemptEventType.PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          // create the topology tables
          .installTopology();

  static class RequestContainerTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      LOG.info("allocate ps agent attempt resource, ps agent id = " + attempt.getId()
          + ", resource = " + attempt.getContext().getPSAgentManager().getPsAgentResource()
          + ", priority = " + attempt.getContext().getPSAgentManager().getPsAgentPriority());

      AngelDeployMode deployMode = attempt.getContext().getDeployMode();
      ContainerAllocatorEvent allocatorEvent = null;

      if (deployMode == AngelDeployMode.LOCAL) {
        allocatorEvent =
            new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ,
                attempt.getId());

      } else {
        allocatorEvent =
            new YarnContainerRequestEvent(attempt.getId(), attempt.getContext().getPSAgentManager()
                    .getPsAgentResource(), attempt.getContext().getPSAgentManager()
                    .getPsAgentPriority(), new String[] {attempt.getContext().getPSAgentManager()
                    .getPsAgent(attempt.getId().getPsAgentId()).getLocation().getIp()});
      }

      attempt.getContext().getEventHandler().handle(allocatorEvent);
    }
  }

  public static class FinishedTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {

    private final PSAgentAttemptState finishState;

    FinishedTransition(PSAgentAttemptState finishState) {
      this.finishState = finishState;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      attempt.setFinishTime();
      // send PS_ATTEMPT_FAILED to AMParameterServer, AMParameterServer will
      // retry another attempt or failed
      switch (finishState) {
        case FAILED:
          attempt
              .getContext()
              .getEventHandler()
              .handle(
                  new PSAgentFromAttemptEvent(AMPSAgentEventType.PSAGENT_ATTEMPT_FAILED, attempt
                      .getId()));
          break;
        case KILLED:
          attempt
              .getContext()
              .getEventHandler()
              .handle(
                  new PSAgentFromAttemptEvent(AMPSAgentEventType.PSAGENT_ATTEMPT_KILLED, attempt
                      .getId()));
          break;
        case SUCCESS:
          attempt
              .getContext()
              .getEventHandler()
              .handle(
                  new PSAgentFromAttemptEvent(AMPSAgentEventType.PSAGENT_ATTEMPT_SUCCESS, attempt
                      .getId()));
          break;
        default:
          LOG.error("invalid PSAttemptStateInternal in PSAttemptFinishedTransition!");
          break;
      }

      attempt.getContext().getMasterService().unRegisterPSAgentAttemptID(attempt.getId());
      // release container
      
      AngelDeployMode deployMode = attempt.getContext().getDeployMode();
      ContainerLauncherEvent launchEvent = null;
      if (deployMode == AngelDeployMode.LOCAL) {
        launchEvent =
            new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP,
                attempt.getId());
      } else {
        launchEvent =
            new YarnContainerLauncherEvent(attempt.getId(), attempt.container.getId(),
                StringInterner.weakIntern(attempt.container.getNodeId().toString()),
                attempt.container.getContainerToken(),
                ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP);
      }
      attempt.getContext().getEventHandler().handle(launchEvent);
    }
  }

  private static class ContainerAssignedTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @SuppressWarnings({"unchecked"})
    @Override
    public void transition(final PSAgentAttempt attempt, PSAgentAttemptEvent event) {

      PSAgentAttemptContainerAssignedEvent assignedEvent =
          (PSAgentAttemptContainerAssignedEvent) event;
      PSAgentAttemptId attemptId = attempt.getId();
      attempt.container = assignedEvent.getContainer();

      AngelDeployMode deployMode = attempt.getContext().getDeployMode();
      ContainerLauncherEvent launchEvent = null;
      if (deployMode == AngelDeployMode.LOCAL) {
        launchEvent =
            new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH,
                attempt.getId());
      } else {
        ContainerLaunchContext launchContext =
            createContainerLaunchContext(attempt.getContext().getContainerAllocator()
                .getApplicationACLs(), attempt.getContext().getConf(), attemptId, attempt
                .getContext().getApplicationId(), attempt.getContext().getMasterService(), attempt
                .getContext().getCredentials());

        launchEvent =
            new ContainerRemoteLaunchEvent(attemptId, launchContext, assignedEvent.getContainer());
      }

      attempt.getContext().getEventHandler().handle(launchEvent);
    }
  }

  private static class DeallocateContainerTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      attempt.setFinishTime();
      AngelDeployMode deployMode = attempt.getContext().getDeployMode();
      ContainerAllocatorEvent allocatorEvent = null;

      if (deployMode == AngelDeployMode.LOCAL) {
        allocatorEvent =
            new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_DEALLOCATE,
                attempt.getId());

      } else {
        allocatorEvent =
            new YarnContainerAllocatorEvent(attempt.getId(),
                ContainerAllocatorEventType.CONTAINER_DEALLOCATE, attempt.context.getPSAgentManager().getPsAgentPriority());
      }
      attempt.getContext().getEventHandler().handle(allocatorEvent);
    }
  }

  private static class StateUpdateTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      // TODO
    }
  }

  private static class LaunchedContainerTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent evnt) {

      // set the launch time
      attempt.launchTime = attempt.getContext().getClock().getTime();
      // set tarckerName,httpPort, which used by webserver
      attempt.trackerName = attempt.container.getNodeHttpAddress();
      LOG.info("trackerName:" + attempt.container.getNodeHttpAddress());
      attempt.httpPort = 8080;

      // added to psManager so psManager can monitor it;
      // psAttempt.getContext().getParameterServerManager.registerPSAttempt(psAttempt.attemptId);

      attempt
          .getContext()
          .getEventHandler()
          .handle(
              new PSAgentFromAttemptEvent(AMPSAgentEventType.PSAGENT_ATTEMPT_LAUNCHED, attempt
                  .getId()));
      attempt.getContext().getMasterService().registerPSAgentAttemptId(attempt.getId());
      LOG.info("has telled attempt started for attempid: " + attempt.getId());
    }
  }

  public static class RegisterTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      PSAgentAttemptRegisterEvent registerEvent = (PSAgentAttemptRegisterEvent) event;
      attempt.location = registerEvent.getLocation();
      attempt.getContext().getLocationManager()
          .setPSAgentLocation(attempt.getId().getPsAgentId(), attempt.location);
      LOG.info(attempt.getId() + " is registering, location: " + attempt.location);
    }
  }

  public static class RegisterForPSModeTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      attempt.getContext().getLocationManager()
          .setPSAgentLocation(attempt.getId().getPsAgentId(), attempt.location);
      LOG.info(attempt.getId() + " is registering, location: " + attempt.location);
    }
  }

  public static class DiagnosticUpdaterTransition implements
      SingleArcTransition<PSAgentAttempt, PSAgentAttemptEvent> {
    @Override
    public void transition(PSAgentAttempt attempt, PSAgentAttemptEvent event) {
      PSAgentAttemptDiagnosticsUpdateEvent diagEvent = (PSAgentAttemptDiagnosticsUpdateEvent) event;
      LOG.info("Diagnostics report from " + attempt.getId() + ": " + diagEvent.getDiagnostics());
      attempt.setDiagnostic(diagEvent.getDiagnostics());
    }
  }


  public boolean isFinished() {
    try {
      readLock.lock();
      PSAgentAttemptState state = stateMachine.getCurrentState();
      return (state == PSAgentAttemptState.SUCCESS || state == PSAgentAttemptState.FAILED || state == PSAgentAttemptState.KILLED);
    } finally {
      readLock.lock();
    }
  }

  public PSAgentAttemptState getState() {
    readLock.lock();
    try {
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  private void setDiagnostic(String diag) {
    diagnostics = diag;
  }

  private void setFinishTime() {
    // set the finish time only if launch time is set
    if (launchTime != 0) {
      finishTime = context.getClock().getTime();
    }
  }

  public AMContext getContext() {
    return context;
  }

  public PSAgentAttemptId getId() {
    return id;
  }

  public String getDiagnostics() {
    try {
      readLock.lock();
      return diagnostics;
    } finally {
      readLock.unlock();
    }
  }

  public Container getContainer() {
    try {
      readLock.lock();
      return container;
    } finally {
      readLock.unlock();
    }
  }

  public long getLaunchTime() {
    try {
      readLock.lock();
      return launchTime;
    } finally {
      readLock.unlock();
    }
  }

  public String getTrackerName() {
    try {
      readLock.lock();
      return trackerName;
    } finally {
      readLock.unlock();
    }
  }

  public int getHttpPort() {
    try {
      readLock.lock();
      return httpPort;
    } finally {
      readLock.unlock();
    }
  }

  public Location getLocation() {
    try {
      readLock.lock();
      return location;
    } finally {
      readLock.unlock();
    }
  }

  public StateMachine<PSAgentAttemptState, PSAgentAttemptEventType, PSAgentAttemptEvent> getStateMachine() {
    return stateMachine;
  }

  public Lock getReadLock() {
    return readLock;
  }

  public Lock getWriteLock() {
    return writeLock;
  }

  public long getFinishTime() {
    try {
      readLock.lock();
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }


  private static LocalResource createLocalResource(FileSystem fc, Path file,
      LocalResourceType type, LocalResourceVisibility visibility) throws IOException {
    FileStatus fstat = fc.getFileStatus(file);
    URL resourceURL = ConverterUtils.getYarnUrlFromPath(fc.resolvePath(fstat.getPath()));
    long resourceSize = fstat.getLen();
    long resourceModificationTime = fstat.getModificationTime();

    return LocalResource.newInstance(resourceURL, type, visibility, resourceSize,
        resourceModificationTime);
  }

  static ContainerLaunchContext createContainerLaunchContext(
      Map<ApplicationAccessType, String> applicationACLs, Configuration conf,
      PSAgentAttemptId attemptId, final ApplicationId appid, MasterService masterService,
      Credentials credentials) {

    synchronized (commonContainerSpecLock) {
      if (commonContainerSpec == null) {
        commonContainerSpec =
            createCommonContainerLaunchContext(masterService, applicationACLs, conf, appid,
                credentials);
      }
    }

    Map<String, String> env = commonContainerSpec.getEnvironment();
    Map<String, String> myEnv = new HashMap<String, String>(env.size());
    myEnv.putAll(env);

    Apps.addToEnvironment(myEnv, AngelEnvironment.PSAGENT_ID.name(),
        Integer.toString(attemptId.getPsAgentId().getIndex()));
    Apps.addToEnvironment(myEnv, AngelEnvironment.PSAGENT_ATTEMPT_ID.name(),
        Integer.toString(attemptId.getIndex()));

    //ParameterServerJVM.setVMEnv(myEnv, conf);

    // Set up the launch command
    List<String> commands = PSAgentAttemptJVM.getVMCommand(conf, appid, attemptId);

    // Duplicate the ByteBuffers for access by multiple containers.
    Map<String, ByteBuffer> myServiceData = new HashMap<String, ByteBuffer>();
    for (Entry<String, ByteBuffer> entry : commonContainerSpec.getServiceData().entrySet()) {
      myServiceData.put(entry.getKey(), entry.getValue().duplicate());
    }

    // Construct the actual Container
    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(commonContainerSpec.getLocalResources(), myEnv,
            commands, myServiceData, commonContainerSpec.getTokens().duplicate(), applicationACLs);

    return container;
  }

  private static ContainerLaunchContext createCommonContainerLaunchContext(
      MasterService masterService, Map<ApplicationAccessType, String> applicationACLs,
      Configuration conf, final ApplicationId appid, Credentials credentials) {

    // Application resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // Application environment
    Map<String, String> environment = new HashMap<String, String>();

    // Service data
    Map<String, ByteBuffer> serviceData = new HashMap<String, ByteBuffer>();

    // Tokens
    ByteBuffer taskCredentialsBuffer = ByteBuffer.wrap(new byte[] {});
    try {
      FileSystem remoteFS = FileSystem.get(conf);

      // Set up JobConf to be localized properly on the remote NM.
      Path remoteJobSubmitDir = new Path(conf.get(AngelConfiguration.ANGEL_JOB_DIR));
      Path remoteJobConfPath = new Path(remoteJobSubmitDir, AngelConfiguration.ANGEL_JOB_CONF_FILE);
      localResources.put(
          AngelConfiguration.ANGEL_JOB_CONF_FILE,
          createLocalResource(remoteFS, remoteJobConfPath, LocalResourceType.FILE,
              LocalResourceVisibility.APPLICATION));
      LOG.info("The job-conf file on the remote FS is " + remoteJobConfPath.toUri().toASCIIString());

      // Setup DistributedCache
      AngelApps.setupDistributedCache(conf, localResources);

      // Setup up task credentials buffer
      LOG.info("Adding #" + credentials.numberOfTokens() + " tokens and #"
          + credentials.numberOfSecretKeys() + " secret keys for NM use for launching container");

      Credentials taskCredentials = new Credentials(credentials);

      DataOutputBuffer containerTokens_dob = new DataOutputBuffer();
      LOG.info("Size of containertokens_dob is " + taskCredentials.numberOfTokens());
      taskCredentials.writeTokenStorageToStream(containerTokens_dob);
      taskCredentialsBuffer =
          ByteBuffer.wrap(containerTokens_dob.getData(), 0, containerTokens_dob.getLength());

      InetSocketAddress listenAddr = masterService.getRPCListenAddr();

      Apps.addToEnvironment(environment, AngelEnvironment.LISTEN_ADDR.name(), listenAddr
          .getAddress().getHostAddress());

      Apps.addToEnvironment(environment, AngelEnvironment.LISTEN_PORT.name(),
          String.valueOf(listenAddr.getPort()));

      Apps.addToEnvironment(environment, Environment.CLASSPATH.name(), getInitialClasspath(conf));

      if (initialAppClasspath != null) {
        Apps.addToEnvironment(environment, Environment.APP_CLASSPATH.name(), initialAppClasspath);
      }

      Apps.addToEnvironment(environment, AngelEnvironment.INIT_MIN_CLOCK.name(), "0");
    } catch (IOException e) {
      throw new YarnRuntimeException(e);
    }

    // Add pwd to LD_LIBRARY_PATH, add this before adding anything else
    Apps.addToEnvironment(environment, Environment.LD_LIBRARY_PATH.name(), Environment.PWD.$());

    ContainerLaunchContext container =
        ContainerLaunchContext.newInstance(localResources, environment, null, serviceData,
            taskCredentialsBuffer, applicationACLs);

    return container;
  }

  private static String getInitialClasspath(Configuration conf) throws IOException {
    synchronized (classpathLock) {
      if (initialClasspathFlag.get()) {
        return initialClasspath;
      }
      Map<String, String> env = new HashMap<String, String>();
      AngelApps.setClasspath(env, conf);
      initialClasspath = env.get(Environment.CLASSPATH.name());
      initialAppClasspath = env.get(Environment.APP_CLASSPATH.name());
      initialClasspathFlag.set(true);
      return initialClasspath;
    }
  }

  public String getNodeHttpAddr() {
    if (container == null) {
      return null;
    } else {
      return container.getNodeHttpAddress();
    }
  }

  public String getContainerIdStr() {
    if (container == null) {
      return null;
    } else {
      return container.getId().toString();
    }
  }

  @Override
  public void handle(PSAgentAttemptEvent event) {
    LOG.debug("Processing " + event.getId() + " of type " + event.getType());
    writeLock.lock();
    try {
      final PSAgentAttemptState oldState = getState();
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
}
