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

package com.tencent.angel.master.worker.attempt;

import com.tencent.angel.AngelDeployMode;
import com.tencent.angel.common.location.Location;
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
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.worker.AMWorkerEventType;
import com.tencent.angel.master.worker.worker.WorkerFromAttemptEvent;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.yarn.util.ContainerContextUtils;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskStateProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportRequest;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.InvalidStateTransitonException;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A running attempt for a worker.
 */
public class WorkerAttempt implements EventHandler<WorkerAttemptEvent> {

  private static final Log LOG = LogFactory.getLog(WorkerAttempt.class);
  /** worker attempt id */
  private final WorkerAttemptId id;
  private final AMContext context;
  private final Lock readLock;
  private final Lock writeLock;

  /** task ids which this worker contains */
  private final List<TaskId> taskIds;

  /** worker attempt metrics */
  private final Map<String, String> metrics;

  /** worker attempt running address(ip and port) */
  private Location location;

  /** container allocated for this worker attempt */
  private Container container;

  /** worker attempt launch time */
  private long launchTime;

  /** worker attempt finish time */
  private long finishTime;

  /** worker attempt diagnostics */
  private final List<String> diagnostics;

  private final StateMachine<WorkerAttemptState, WorkerAttemptEventType, WorkerAttemptEvent> stateMachine;

  public WorkerAttempt(WorkerId workerId, int attemptIndex, AMContext context,
      List<TaskId> taskIds, WorkerAttempt workerAttempt) {
    this.context = context;
    this.id = new WorkerAttemptId(workerId, attemptIndex);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.metrics = new HashMap<String, String>();
    this.stateMachine = stateMachineFactory.make(this);
    this.diagnostics = new ArrayList<String>();
    this.taskIds = taskIds;

    // if workerAttempt is not null, we should clone task state from it
    if (workerAttempt == null) {
      int size = taskIds.size();
      for (int i = 0; i < size; i++) {
        if (context.getTaskManager().getTask(taskIds.get(i)) == null) {
          AMTask task = new AMTask(taskIds.get(i), null);
          context.getTaskManager().putTask(taskIds.get(i), task);
        }
      }
    } else {
      Map<TaskId, AMTask> oldTaskMap = workerAttempt.getTaskMap();
      for (Entry<TaskId, AMTask> taskEntry : oldTaskMap.entrySet()) {
        LOG.debug("clone task, taskId=" + taskEntry.getKey() + ", task=" + taskEntry.getValue());
        TaskId taskId = taskEntry.getKey();
        AMTask task = new AMTask(taskEntry.getKey(), taskEntry.getValue());
        context.getTaskManager().putTask(taskId, task);
      }
    }
  }

  private static final DiagnosticUpdaterTransition DIAGNOSTIC_UPDATE_TRANSITION =
      new DiagnosticUpdaterTransition();

  private static final WorkerAttemptNewFailedTransition NEW_FAILED_TRANSITION =
      new WorkerAttemptNewFailedTransition();
  private static final WorkerAttemptNewKilledTransition NEW_KILLED_TRANSITION =
      new WorkerAttemptNewKilledTransition();

  private static final WorkerAttemptAssignedFailedTransition ASSIGNED_FAILED_TRANSITION =
      new WorkerAttemptAssignedFailedTransition();
  private static final WorkerAttemptAssignedKilledTransition ASSIGNED_KILLED_TRANSITION =
      new WorkerAttemptAssignedKilledTransition();

  private static final WorkerAttemptRunningFailedTransition RUNNING_FAILED_TRANSITION =
      new WorkerAttemptRunningFailedTransition();
  private static final WorkerAttemptRunningKilledTransition RUNNING_KILLED_TRANSITION =
      new WorkerAttemptRunningKilledTransition();

  private static final WorkerAttemptDoneTransition DONE_TRANSITION =
      new WorkerAttemptDoneTransition();

  protected static final StateMachineFactory<WorkerAttempt, WorkerAttemptState, WorkerAttemptEventType, WorkerAttemptEvent> stateMachineFactory =
      new StateMachineFactory<WorkerAttempt, WorkerAttemptState, WorkerAttemptEventType, WorkerAttemptEvent>(
          WorkerAttemptState.NEW)

          // from NEW state
          .addTransition(WorkerAttemptState.NEW, WorkerAttemptState.SCHEDULED,
              WorkerAttemptEventType.SCHEDULE, new RequestContainerTransition())
          .addTransition(WorkerAttemptState.NEW, WorkerAttemptState.KILLED,
              WorkerAttemptEventType.KILL, NEW_KILLED_TRANSITION)
          .addTransition(WorkerAttemptState.NEW, WorkerAttemptState.FAILED,
              WorkerAttemptEventType.ERROR, NEW_FAILED_TRANSITION)
          .addTransition(WorkerAttemptState.NEW, WorkerAttemptState.NEW,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          // from SCHEDULED state
          .addTransition(WorkerAttemptState.SCHEDULED, WorkerAttemptState.ASSIGNED,
              WorkerAttemptEventType.CONTAINER_ASSIGN, new AssignContainerTransition())
          .addTransition(WorkerAttemptState.SCHEDULED, WorkerAttemptState.FAILED,
              WorkerAttemptEventType.ERROR, ASSIGNED_FAILED_TRANSITION)
          .addTransition(WorkerAttemptState.SCHEDULED, WorkerAttemptState.KILLED,
              WorkerAttemptEventType.KILL, ASSIGNED_KILLED_TRANSITION)
          .addTransition(WorkerAttemptState.SCHEDULED, WorkerAttemptState.SCHEDULED,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(WorkerAttemptState.ASSIGNED, WorkerAttemptState.LAUNCHED,
              WorkerAttemptEventType.CONTAINER_LAUNCHED, new ContainerLaunchedTransition())
          .addTransition(
              WorkerAttemptState.ASSIGNED,
              WorkerAttemptState.FAILED,
              EnumSet.of(WorkerAttemptEventType.CONTAINER_LAUNCH_FAILED,
                  WorkerAttemptEventType.ERROR), ASSIGNED_FAILED_TRANSITION)
          .addTransition(WorkerAttemptState.ASSIGNED, WorkerAttemptState.KILLED,
              WorkerAttemptEventType.KILL, ASSIGNED_KILLED_TRANSITION)
          .addTransition(WorkerAttemptState.ASSIGNED, WorkerAttemptState.ASSIGNED,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(WorkerAttemptState.LAUNCHED, WorkerAttemptState.RUNNING,
              WorkerAttemptEventType.REGISTER, new RegisterTransition())
          .addTransition(WorkerAttemptState.LAUNCHED, WorkerAttemptState.FAILED,
              EnumSet.of(WorkerAttemptEventType.ERROR, WorkerAttemptEventType.CONTAINER_COMPLETE),
              RUNNING_FAILED_TRANSITION)
          .addTransition(WorkerAttemptState.LAUNCHED, WorkerAttemptState.KILLED,
              WorkerAttemptEventType.KILL, RUNNING_KILLED_TRANSITION)
          .addTransition(WorkerAttemptState.LAUNCHED, WorkerAttemptState.LAUNCHED,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(WorkerAttemptState.RUNNING, WorkerAttemptState.RUNNING,
              WorkerAttemptEventType.REGISTER)
          .addTransition(WorkerAttemptState.RUNNING, WorkerAttemptState.RUNNING,
              WorkerAttemptEventType.UPDATE_STATE, new StateUpdateTransition())
          .addTransition(WorkerAttemptState.RUNNING, WorkerAttemptState.FAILED,
              EnumSet.of(WorkerAttemptEventType.ERROR, WorkerAttemptEventType.CONTAINER_COMPLETE),
              RUNNING_FAILED_TRANSITION)
          .addTransition(WorkerAttemptState.RUNNING, WorkerAttemptState.KILLED,
              WorkerAttemptEventType.KILL, RUNNING_KILLED_TRANSITION)
          .addTransition(WorkerAttemptState.RUNNING, WorkerAttemptState.SUCCESS,
              WorkerAttemptEventType.DONE, DONE_TRANSITION)
          .addTransition(WorkerAttemptState.RUNNING, WorkerAttemptState.RUNNING,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              WorkerAttemptState.KILLED,
              WorkerAttemptState.KILLED,
              EnumSet.of(WorkerAttemptEventType.KILL, WorkerAttemptEventType.REGISTER,
                  WorkerAttemptEventType.UNREGISTER, WorkerAttemptEventType.CONTAINER_ASSIGN,
                  WorkerAttemptEventType.UPDATE_STATE, WorkerAttemptEventType.DONE,
                  WorkerAttemptEventType.COMMIT_FAILED, WorkerAttemptEventType.ERROR,
                  WorkerAttemptEventType.CONTAINER_COMPLETE))
          .addTransition(WorkerAttemptState.KILLED, WorkerAttemptState.KILLED,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              WorkerAttemptState.FAILED,
              WorkerAttemptState.FAILED,
              EnumSet.of(WorkerAttemptEventType.KILL, WorkerAttemptEventType.REGISTER,
                  WorkerAttemptEventType.SCHEDULE, WorkerAttemptEventType.UNREGISTER,
                  WorkerAttemptEventType.CONTAINER_ASSIGN, WorkerAttemptEventType.DONE,
                  WorkerAttemptEventType.UPDATE_STATE, WorkerAttemptEventType.COMMIT_FAILED,
                  WorkerAttemptEventType.ERROR, WorkerAttemptEventType.CONTAINER_COMPLETE))
          .addTransition(WorkerAttemptState.FAILED, WorkerAttemptState.FAILED,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              WorkerAttemptState.SUCCESS,
              WorkerAttemptState.SUCCESS,
              EnumSet.of(WorkerAttemptEventType.KILL, WorkerAttemptEventType.REGISTER,
                  WorkerAttemptEventType.SCHEDULE, WorkerAttemptEventType.UNREGISTER,
                  WorkerAttemptEventType.CONTAINER_ASSIGN, WorkerAttemptEventType.UPDATE_STATE,
                  WorkerAttemptEventType.DONE, WorkerAttemptEventType.COMMIT_FAILED,
                  WorkerAttemptEventType.ERROR, WorkerAttemptEventType.CONTAINER_COMPLETE))
          .addTransition(WorkerAttemptState.SUCCESS, WorkerAttemptState.SUCCESS,
              WorkerAttemptEventType.DIAGNOSTICS_UPDATE, DIAGNOSTIC_UPDATE_TRANSITION);

  private static class DiagnosticUpdaterTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {
    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      WorkerAttemptDiagnosticsUpdateEvent diagEvent = (WorkerAttemptDiagnosticsUpdateEvent) event;
      LOG.info("Diagnostics report from " + attempt.getId() + ": " + diagEvent.getDiagnostics());
      attempt.diagnostics.add(diagEvent.getDiagnostics());
    }
  }

  private static class RequestContainerTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {
    @SuppressWarnings("unchecked")
    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // get data splits location for data locality
      AMWorkerGroup workerGroup =
          attempt.context.getWorkerManager().getWorkerGroup(attempt.getId().getWorkerId());
      String[] hosts =
          attempt.context.getDataSpliter().getSplitLocations(workerGroup.getSplitIndex());

      LOG.info("allocate worker attempt resource, worker attempt id = " + attempt.getId()
          + ", resource = " + attempt.getContext().getWorkerManager().getWorkerResource()
          + ", priority = " + attempt.getContext().getWorkerManager().getWorkerPriority()
          + ", hosts = " + StringUtils.arrayToString(hosts));

      // reqeuest resource:send a resource request to the resource allocator
      AngelDeployMode deployMode = attempt.getContext().getDeployMode();
      ContainerAllocatorEvent allocatorEvent = null;

      if (deployMode == AngelDeployMode.LOCAL) {
        allocatorEvent =
            new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_REQ,
                attempt.getId());

      } else {
        allocatorEvent =
            new YarnContainerRequestEvent(attempt.getId(), attempt.getContext().getWorkerManager()
                .getWorkerResource(), attempt.getContext().getWorkerManager().getWorkerPriority(),
                hosts);
      }
      attempt.getContext().getEventHandler().handle(allocatorEvent);
    }
  }

  private static class AssignContainerTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      WorkerAttemptContainerAssignedEvent assignedEvent =
          (WorkerAttemptContainerAssignedEvent) event;
      WorkerAttemptId attemptId = attempt.getId();
      attempt.container = assignedEvent.getContainer();

      // once the resource is applied, build and send the launch request to the container launcher
      AngelDeployMode deployMode = attempt.getContext().getDeployMode();
      ContainerLauncherEvent launchEvent = null;

      if (deployMode == AngelDeployMode.LOCAL) {
        launchEvent =
            new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_LAUNCH,
                attempt.getId());
      } else {
        ContainerLaunchContext launchContext =
            ContainerContextUtils.createContainerLaunchContext(attempt.getContext()
                .getContainerAllocator().getApplicationACLs(), attempt.getContext().getConf(),
                attemptId, 0, attempt.getContext().getApplicationId(), attempt.getContext()
                    .getMasterService(), attempt.getContext().getCredentials());

        launchEvent =
            new ContainerRemoteLaunchEvent(attemptId, launchContext, assignedEvent.getContainer());
      }

      attempt.getContext().getEventHandler().handle(launchEvent);
    }
  }

  private static class WorkerAttemptNewFailedTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // notify failed message to the worker
      attempt.notifyWorkerAttemptFailed();
    }
  }

  private static class WorkerAttemptNewKilledTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // notify killed message to the worker
      attempt.notifyWorkerAttemptKilled();
    }
  }

  private static class WorkerAttemptAssignedFailedTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // release the allocated container
      attempt.deallocContainer();
      // notify failed message to the worker
      attempt.notifyWorkerAttemptFailed();
      // remove the worker attempt from heartbeat timeout listen list
      attempt.unregisterFromHeartBeatListers();
    }
  }

  private static class WorkerAttemptAssignedKilledTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // release the allocated container
      attempt.deallocContainer();
      // notify killed message to the worker
      attempt.notifyWorkerAttemptKilled();
      // remove the worker attempt from heartbeat timeout listen list
      attempt.unregisterFromHeartBeatListers();
    }
  }

  private static class WorkerAttemptRunningFailedTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // clean the container
      attempt.cleanContainer();
      // notify failed message to the worker
      attempt.notifyWorkerAttemptFailed();
      // remove the worker attempt from heartbeat timeout listen list
      attempt.unregisterFromHeartBeatListers();
      // record the finish time
      attempt.setFinishTime();
    }
  }

  private static class WorkerAttemptRunningKilledTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // clean the container
      attempt.cleanContainer();
      // notify killed message to the worker
      attempt.notifyWorkerAttemptKilled();
      // remove the worker attempt from heartbeat timeout listening list
      attempt.unregisterFromHeartBeatListers();
      // record the finish time
      attempt.setFinishTime();
    }
  }

  private static class ContainerLaunchedTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {
    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      LOG.info("add " + attempt.getId() + " to monitor!");
      // if the worker attempt launch successfully, add it to heartbeat timeout listening list
      attempt.getContext().getWorkerManager().register(attempt.getId());
    }
  }

  private static class RegisterTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {
    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      WorkerAttemptRegisterEvent rEvent = (WorkerAttemptRegisterEvent) event;
      // set worker attempt location
      attempt.location = rEvent.getLocation();
      LOG.info("worker attempt: " + attempt.getId() + " is registering, location = "
          + rEvent.getLocation());
      // notify the register message to the worker
      attempt.notifyWorkerAttemptRegisted();
      // record the launch time
      attempt.setLaunchTime();
    }
  }

  private static class StateUpdateTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {

    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      WorkerAttemptStateUpdateEvent updateEvent = (WorkerAttemptStateUpdateEvent) event;
      WorkerReportRequest report = updateEvent.getReport();

      // update worker attempt metrics
      for (Pair pair : report.getPairsList()) {
        attempt.metrics.put(pair.getKey(), pair.getValue());
      }

      // update tasks metrics
      List<TaskStateProto> taskReports = report.getTaskReportsList();
      int size = taskReports.size();
      for (int i = 0; i < size; i++) {
        TaskStateProto taskState = taskReports.get(i);
        AMTask task =
            attempt.getContext().getTaskManager()
                .getTask(ProtobufUtil.convertToId(taskState.getTaskId()));
        task.updateTaskState(taskState);
      }
    }
  }

  private static class WorkerAttemptDoneTransition implements
      SingleArcTransition<WorkerAttempt, WorkerAttemptEvent> {
    @Override
    public void transition(WorkerAttempt attempt, WorkerAttemptEvent event) {
      // clean the container
      attempt.cleanContainer();
      // notify the worker attempt run successfully message to the worker
      attempt.notifyWorkerAttemptDone();
      // record the finish time
      attempt.setFinishTime();
    }
  }

  @SuppressWarnings("unchecked")
  private void cleanContainer() {
    AngelDeployMode deployMode = context.getDeployMode();
    ContainerLauncherEvent launchEvent = null;

    if (deployMode == AngelDeployMode.LOCAL) {
      launchEvent =
          new LocalContainerLauncherEvent(ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP, id);
    } else {
      launchEvent =
          new YarnContainerLauncherEvent(id, container.getId(), StringInterner.weakIntern(container
              .getNodeId().toString()), container.getContainerToken(),
              ContainerLauncherEventType.CONTAINER_REMOTE_CLEANUP);
    }
    context.getEventHandler().handle(launchEvent);
  }

  @SuppressWarnings("unchecked")
  private void deallocContainer() {
    LOG.info("release container:" + container);
    AngelDeployMode deployMode = context.getDeployMode();
    ContainerAllocatorEvent allocatorEvent = null;

    if (deployMode == AngelDeployMode.LOCAL) {
      allocatorEvent =
          new LocalContainerAllocatorEvent(ContainerAllocatorEventType.CONTAINER_DEALLOCATE, id);
    } else {
      allocatorEvent =
          new YarnContainerAllocatorEvent(id, ContainerAllocatorEventType.CONTAINER_DEALLOCATE,
              context.getWorkerManager().getWorkerPriority());
    }
    context.getEventHandler().handle(allocatorEvent);
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerAttemptFailed() {
    context.getEventHandler().handle(
        new WorkerFromAttemptEvent(AMWorkerEventType.WORKER_ATTEMPT_FAILED, id));
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerAttemptKilled() {
    context.getEventHandler().handle(
        new WorkerFromAttemptEvent(AMWorkerEventType.WORKER_ATTEMPT_KILLED, id));
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerAttemptRegisted() {
    context.getEventHandler().handle(
        new WorkerFromAttemptEvent(AMWorkerEventType.WORKER_ATTEMPT_REGISTED, id));
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerAttemptDone() {
    context.getEventHandler().handle(
        new WorkerFromAttemptEvent(AMWorkerEventType.WORKER_ATTEMPT_SUCCESS, id));
  }

  private void setFinishTime() {
    if (launchTime != 0) {
      finishTime = context.getClock().getTime();
    }
  }

  private void setLaunchTime() {
    launchTime = context.getClock().getTime();
  }

  @Override
  public void handle(WorkerAttemptEvent event) {
    LOG.debug("Processing " + event.getWorkerAttemptId() + " of type " + event.getType());

    writeLock.lock();
    try {
      final WorkerAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error(e.getMessage());
      }
      if (oldState != getState()) {
        LOG.info(event.getWorkerAttemptId() + " psserver Transitioned from " + oldState + " to "
            + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * get worker attempt metrics
   * 
   * @return Map<String, String> worker attempt metrics
   */
  public Map<String, String> getMetrics() {
    try {
      readLock.lock();
      Map<String, String> cloneMetrics = new HashMap<String, String>(metrics.size());
      cloneMetrics.putAll(metrics);
      return cloneMetrics;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the container allocated for this worker attempt
   * 
   * @return Container the container allocated for this worker attempt
   */
  public Container getContainer() {
    try {
      readLock.lock();
      return container;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the worker attempt id
   * 
   * @return WorkerAttemptId the worker attempt id
   */
  public WorkerAttemptId getId() {
    return id;
  }

  public AMContext getContext() {
    return context;
  }

  /**
   * get the worker attempt state
   * 
   * @return WorkerAttemptState the worker attempt state
   */
  public WorkerAttemptState getState() {
    try {
      readLock.lock();
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the tasks running on this worker attempt
   * 
   * @return Map<TaskId, AMTask> the tasks running on this worker attempt
   */
  public Map<TaskId, AMTask> getTaskMap() {
    int size = taskIds.size();
    Map<TaskId, AMTask> taskMap = new HashMap<TaskId, AMTask>(size);
    for (int i = 0; i < size; i++) {
      taskMap.put(taskIds.get(i), context.getTaskManager().getTask(taskIds.get(i)));
    }
    return taskMap;
  }

  /**
   * get the worker attempt launch time
   * 
   * @return long the worker attempt launch time
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
   * get the worker attempt finish time
   * 
   * @return long the worker attempt finish time
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
   * get the worker attempt location
   * 
   * @return Location the worker attempt location
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
   * get the worker attempt diagnostics
   * 
   * @return List<String> the worker attempt diagnostics
   */
  public List<String> getDiagnostics() {
    try {
      readLock.lock();
      List<String> ret = new ArrayList<String>();
      ret.addAll(diagnostics);
      return ret;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * check if the worker attempt run over
   * 
   * @return boolean
   */
  public boolean isFinished() {
    try {
      readLock.lock();
      WorkerAttemptState state = stateMachine.getCurrentState();
      return (state == WorkerAttemptState.SUCCESS || state == WorkerAttemptState.FAILED || state == WorkerAttemptState.KILLED);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the web address of the node the attempt is running on
   * 
   * @return String the web address of the node the attempt is running on
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
   * get the container id string
   * 
   * @return String the container id string
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

  private void unregisterFromHeartBeatListers() {
    context.getWorkerManager().unRegister(id);
  }

  /**
   * get the minimal iteration values in all the tasks contained in this worker attempt
   * 
   * @return int the minimal iteration values in all the tasks contained in this worker attempt
   */
  public int getMinIteration() {
    int minIteration = Integer.MAX_VALUE;
    int size = taskIds.size();
    AMTask task = null;
    for (int i = 0; i < size; i++) {
      task = context.getTaskManager().getTask(taskIds.get(i));
      if (task.getIteration() < minIteration) {
        minIteration = task.getIteration();
      }
    }
    return minIteration;
  }

  /**
   * Get worker attempt log url
   * @return worker log url
   */
  public String getLogUrl() {
    if(location == null || container == null) {
      return "";
    } else {
      return "http://" + location.getIp() + ":" + context.getYarnNMWebPort() + "/node/containerlogs/"
        + container.getId() + "/angel/syslog/?start=0";
    }
  }
}
