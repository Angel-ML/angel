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

package com.tencent.angel.master.worker.workergroup;

import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.worker.WorkerGroupManagerEvent;
import com.tencent.angel.master.worker.WorkerManagerEventType;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.worker.AMWorkerEvent;
import com.tencent.angel.master.worker.worker.AMWorkerEventType;
import com.tencent.angel.master.worker.worker.AMWorkerState;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
 * WorkerGroup in Angel is used to implement model parallelism, it is a virtual group that contains one or more workers.
 * The workers in one group is calculate different parts or a big model use a same training data. But now, Angel only
 * support one worker in a WorkerGroup.
 */
public class AMWorkerGroup implements EventHandler<AMWorkerGroupEvent> {
  private static final Log LOG = LogFactory.getLog(AMWorkerGroup.class);
  private final static KillWorkerGroupTransition KILL_TRANSITION = new KillWorkerGroupTransition();
  private final static FailedWorkerGroupTransition FAILED_TRANSITION =
      new FailedWorkerGroupTransition();
  private final static DiagnosticUpdaterTransition DIAGNOSTIC_UPDATE_TRANSITION =
      new DiagnosticUpdaterTransition();
  protected static final StateMachineFactory<AMWorkerGroup, AMWorkerGroupState, AMWorkerGroupEventType, AMWorkerGroupEvent> stateMachineFactory =
      new StateMachineFactory<AMWorkerGroup, AMWorkerGroupState, AMWorkerGroupEventType, AMWorkerGroupEvent>(
          AMWorkerGroupState.NEW)
          .addTransition(
              AMWorkerGroupState.NEW,
              AMWorkerGroupState.INITED,
              AMWorkerGroupEventType.INIT)
          .addTransition(
              AMWorkerGroupState.NEW,
              AMWorkerGroupState.KILLED,
              AMWorkerGroupEventType.KILL,
              KILL_TRANSITION)
          .addTransition(
              AMWorkerGroupState.NEW,
              AMWorkerGroupState.FAILED,
              AMWorkerGroupEventType.ERROR,
              FAILED_TRANSITION)
          .addTransition(
              AMWorkerGroupState.NEW,
              AMWorkerGroupState.NEW,
              AMWorkerGroupEventType.DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              AMWorkerGroupState.INITED,
              AMWorkerGroupState.INITED,
              AMWorkerGroupEventType.WORKER_REGISTED,
              new WorkerRegistedTransition())
          .addTransition(
              AMWorkerGroupState.INITED,
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupEventType.REGISTED,
              new WorkerGroupRegistedTransition())
          .addTransition(
              AMWorkerGroupState.INITED,
              AMWorkerGroupState.KILLED,
              EnumSet.of(
                  AMWorkerGroupEventType.KILL,
                  AMWorkerGroupEventType.WORKER_KILL),
              KILL_TRANSITION)
          .addTransition(
              AMWorkerGroupState.INITED,
              AMWorkerGroupState.FAILED,
              EnumSet.of(
                  AMWorkerGroupEventType.ERROR,
                  AMWorkerGroupEventType.WORKER_ERROR),
              FAILED_TRANSITION)
          .addTransition(
              AMWorkerGroupState.INITED,
              AMWorkerGroupState.INITED,
              AMWorkerGroupEventType.DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupEventType.WORKER_DONE,
              new WorkerDoneTransition())
          .addTransition(
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupState.SUCCESS,
              AMWorkerGroupEventType.DONE,
              new WorkerGroupDoneTransition())
          .addTransition(
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupState.KILLED,
              EnumSet.of(
                  AMWorkerGroupEventType.KILL,
                  AMWorkerGroupEventType.WORKER_KILL),
              KILL_TRANSITION)
          .addTransition(
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupState.FAILED,
              EnumSet.of(
                  AMWorkerGroupEventType.WORKER_ERROR,
                  AMWorkerGroupEventType.ERROR),
              FAILED_TRANSITION)
          .addTransition(
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupState.RUNNING,
              AMWorkerGroupEventType.DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              AMWorkerGroupState.KILLED,
              AMWorkerGroupState.KILLED,
              EnumSet.of(
                  AMWorkerGroupEventType.INIT,
                  AMWorkerGroupEventType.DONE,
                  AMWorkerGroupEventType.ERROR,
                  AMWorkerGroupEventType.REGISTED,
                  AMWorkerGroupEventType.KILL,
                  AMWorkerGroupEventType.WORKER_DONE,
                  AMWorkerGroupEventType.WORKER_REGISTED,
                  AMWorkerGroupEventType.WORKER_ERROR,
                  AMWorkerGroupEventType.WORKER_KILL))
          .addTransition(
              AMWorkerGroupState.KILLED,
              AMWorkerGroupState.KILLED,
              AMWorkerGroupEventType.DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              AMWorkerGroupState.FAILED,
              AMWorkerGroupState.FAILED,
              EnumSet.of(
                  AMWorkerGroupEventType.INIT,
                  AMWorkerGroupEventType.DONE,
                  AMWorkerGroupEventType.ERROR,
                  AMWorkerGroupEventType.REGISTED,
                  AMWorkerGroupEventType.KILL,
                  AMWorkerGroupEventType.WORKER_DONE,
                  AMWorkerGroupEventType.WORKER_REGISTED,
                  AMWorkerGroupEventType.WORKER_ERROR,
                  AMWorkerGroupEventType.WORKER_KILL))
          .addTransition(
              AMWorkerGroupState.FAILED,
              AMWorkerGroupState.FAILED,
              AMWorkerGroupEventType.DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION)

          .addTransition(
              AMWorkerGroupState.SUCCESS,
              AMWorkerGroupState.SUCCESS,
              EnumSet.of(
                  AMWorkerGroupEventType.INIT,
                  AMWorkerGroupEventType.DONE,
                  AMWorkerGroupEventType.ERROR,
                  AMWorkerGroupEventType.REGISTED,
                  AMWorkerGroupEventType.KILL,
                  AMWorkerGroupEventType.WORKER_DONE,
                  AMWorkerGroupEventType.WORKER_REGISTED,
                  AMWorkerGroupEventType.WORKER_ERROR,
                  AMWorkerGroupEventType.WORKER_KILL))
          .addTransition(
              AMWorkerGroupState.SUCCESS,
              AMWorkerGroupState.SUCCESS,
              AMWorkerGroupEventType.DIAGNOSTICS_UPDATE,
              DIAGNOSTIC_UPDATE_TRANSITION);

  /**worker group id*/
  private final WorkerGroupId groupId;

  /**workers contained in this worker group*/
  private final Map<WorkerId, AMWorker> workerMap;

  /**success worker set*/
  private final Set<WorkerId> successWorkerSet;

  /**failed worker set*/
  private final Set<WorkerId> failedWorkerSet;

  /**killed worker set*/
  private final Set<WorkerId> killedWorkerSet;

  /**worker leader id, not used now*/
  private final WorkerId leader;
  private final AMContext context;
  private final Lock readLock;
  private final Lock writeLock;

  /**diagnostices of the worker group*/
  private final List<String> diagnostics;

  /**training data block index assgined to this worker group*/
  private final int splitIndex;
  private final StateMachine<AMWorkerGroupState, AMWorkerGroupEventType, AMWorkerGroupEvent> stateMachine;
  private long launchTime;
  private long finishTime;

  /**
   * Create a AMWorkerGroup
   * @param groupId worker group id
   * @param context master context
   * @param workerMap workers contains in worker group
   * @param leader leader worker of worker group
   * @param splitIndex training data block index assgined to this worker group
   */
  public AMWorkerGroup(WorkerGroupId groupId, AMContext context, Map<WorkerId, AMWorker> workerMap,
      WorkerId leader, int splitIndex) {
    this.context = context;
    this.groupId = groupId;
    this.workerMap = workerMap;
    this.leader = leader;
    this.splitIndex = splitIndex;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    stateMachine = stateMachineFactory.make(this);
    diagnostics = new ArrayList<String>();
    successWorkerSet = new HashSet<WorkerId>();
    failedWorkerSet = new HashSet<WorkerId>();
    killedWorkerSet = new HashSet<WorkerId>();
  }

  private void addDiagnosticInfo(String diagnostic) {
    diagnostics.add(diagnostic);
  }

  @Override
  public void handle(AMWorkerGroupEvent event) {
    LOG.debug("Processing " + event.getGroupId() + " of type " + event.getType());

    try {
      writeLock.lock();
      final AMWorkerGroupState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {

      }
      if (oldState != getState()) {
        LOG.info(event.getGroupId() + " psserver Transitioned from " + oldState + " to "
            + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Get worker group state
   * @return worker group state
   */
  public AMWorkerGroupState getState() {
    try {
      readLock.lock();
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get training data block index assgined to this worker group
   * @return training data block index assgined to this worker group
   */
  public int getSplitIndex() {
    return splitIndex;
  }

  /**
   * Get diagnostics
   * @return diagnostics
   */
  public List<String> getDiagnostics() {
    try {
      readLock.lock();
      List<String> cloneDiagnostics = new ArrayList<String>();
      cloneDiagnostics.addAll(diagnostics);
      return cloneDiagnostics;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get leader worker id
   * @return leader worker id
   */
  public WorkerId getLeader() {
    try{
      writeLock.lock();
      return leader;
    } finally {
      writeLock.unlock();
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

  private void setLaunchTime(long launchTime) {
    this.launchTime = launchTime;
  }

  public long getFinishTime() {
    try {
      readLock.lock();
      return finishTime;
    } finally {
      readLock.unlock();
    }
  }

  private void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  /**
   * Get worker id to worker map
   * @return worker id to worker map
   */
  public Map<WorkerId, AMWorker> getWorkerMap() {
    return workerMap;
  }

  /**
   * Get the ids of workers contained in worker group
   * @return the ids of workers contained in worker group
   */
  public Set<WorkerId> getWorkerIdSet() {
    return workerMap.keySet();
  }

  /**
   * Get workers contained in worker group
   * @return workers contained in worker group
   */
  public Collection<AMWorker> getWorkerSet() {
    return workerMap.values();
  }

  /**
   * Get worker group id
   * @return worker group id
   */
  public WorkerGroupId getId() {
    return groupId;
  }

  /**
   * Get worker use a worker id
   * @param id worker id
   * @return worker
   */
  public AMWorker getWorker(WorkerId id) {
    return workerMap.get(id);
  }

  public AMContext getContext() {
    return context;
  }

  /**
   * Is the worker group finished
   * @return true means this worker group is finish, else means not
   */
  public boolean isFinished() {
    try {
      readLock.lock();
      AMWorkerGroupState state = getState();
      return state == AMWorkerGroupState.FAILED || state == AMWorkerGroupState.SUCCESS
          || state == AMWorkerGroupState.KILLED;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get the minimal iteration value of the tasks running in this worker group
   * @return the minimal iteration value of the tasks running in this worker group
   */
  public int getMinIteration() {
    int minIteration = Integer.MAX_VALUE;
    for(AMWorker worker:workerMap.values()){
      int workerMinIteration = worker.getMinIteration();
      if(workerMinIteration < minIteration){
        minIteration = workerMinIteration;
      }
    }

    return minIteration;
  }

  private static class KillWorkerGroupTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      if(event.getType() == AMWorkerGroupEventType.WORKER_KILL){
        group.killedWorkerSet.add(((WorkerGroupFromWorkerEvent) event).getWorkerId());
      }

      for (WorkerId workerId : group.getWorkerIdSet()) {
        group.getContext().getEventHandler()
            .handle(new AMWorkerEvent(AMWorkerEventType.KILL, workerId));
      }
      group
          .getContext()
          .getEventHandler()
          .handle(
              new WorkerGroupManagerEvent(WorkerManagerEventType.WORKERGROUP_KILLED, group
                  .getId()));

      if (group.getLaunchTime() != 0) {
        group.setFinishTime(System.currentTimeMillis());
      }
    }
  }

  private static class FailedWorkerGroupTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      if(event.getType() == AMWorkerGroupEventType.WORKER_ERROR){
        group.failedWorkerSet.add(((WorkerGroupFromWorkerEvent) event).getWorkerId());
      }

      for (WorkerId workerId : group.getWorkerIdSet()) {
        group.getContext().getEventHandler()
            .handle(new AMWorkerEvent(AMWorkerEventType.KILL, workerId));
      }

      group
          .getContext()
          .getEventHandler()
          .handle(
              new WorkerGroupManagerEvent(WorkerManagerEventType.WORKERGROUP_FAILED, group
                  .getId()));

      if (group.getLaunchTime() != 0) {
        group.setFinishTime(System.currentTimeMillis());
      }
    }
  }

  private static class DiagnosticUpdaterTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      WorkerGroupDiagnosticsUpdateEvent diagEvent = (WorkerGroupDiagnosticsUpdateEvent) event;
      LOG.info("Diagnostics report from " + group.getId() + ": "
          + diagEvent.getDiagnostic());
      group.addDiagnosticInfo(diagEvent.getDiagnostic());
    }

  }

  private static class WorkerRegistedTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      int runningNum = 0;
      for (Entry<WorkerId, AMWorker> entry : group.getWorkerMap().entrySet()) {
        if (entry.getValue().getState() != AMWorkerState.RUNNING) {
          break;
        }
        runningNum++;
      }

      if (runningNum == group.getWorkerMap().size()) {
        LOG.info("now all workers in workerGroup " + group.groupId + " are registered!");
        group
            .getContext()
            .getEventHandler()
            .handle(
                new AMWorkerGroupEvent(AMWorkerGroupEventType.REGISTED, group.getId()));
      }
    }
  }

  private static class WorkerGroupRegistedTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      group.setLaunchTime(System.currentTimeMillis());
    }
  }

  private static class WorkerDoneTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      WorkerGroupFromWorkerEvent workerEvent = (WorkerGroupFromWorkerEvent) event;
      group.successWorkerSet.add(workerEvent.getWorkerId());

      if (group.successWorkerSet.size() == group.getWorkerMap().size()) {
        group.getContext().getEventHandler()
            .handle(new AMWorkerGroupEvent(AMWorkerGroupEventType.DONE, group.getId()));
      }
    }
  }

  private static class WorkerGroupDoneTransition implements
      SingleArcTransition<AMWorkerGroup, AMWorkerGroupEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMWorkerGroup group, AMWorkerGroupEvent event) {
      group
          .getContext()
          .getEventHandler()
          .handle(
              new WorkerGroupManagerEvent(WorkerManagerEventType.WORKERGROUP_DONE, group
                  .getId()));
      if (group.getLaunchTime() != 0) {
        group.setFinishTime(System.currentTimeMillis());
      }
    }
  }
}
