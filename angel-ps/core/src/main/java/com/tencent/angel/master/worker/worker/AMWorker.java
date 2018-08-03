/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.master.worker.worker;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.task.AMTask;
import com.tencent.angel.master.worker.attempt.WorkerAttempt;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupEventType;
import com.tencent.angel.master.worker.workergroup.WorkerGroupDiagnosticsUpdateEvent;
import com.tencent.angel.master.worker.workergroup.WorkerGroupFromWorkerEvent;
import com.tencent.angel.utils.StringUtils;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for a single worker, it manages all run attempts for the worker.
 * {@link com.tencent.angel.master.worker.attempt.WorkerAttempt} means a worker running attempt. Once the
 * running attempt for the worker is failed or killed, it will initialize and startup a new run attempt
 * for the worker if the number of failed/killed run attempts less than maximum number of failures
 * allowed
 */
public class AMWorker implements EventHandler<AMWorkerEvent> {

  private static final Log LOG = LogFactory.getLog(AMWorker.class);
  
  /**worker id*/
  private final WorkerId id;
  private final AMContext context;
  private final Lock readLock;
  private final Lock writeLock;
  
  /**task ids which this worker contains*/
  private final List<TaskId> taskIds;
  
  /**worker metrics*/
  private final Map<String, String> metrics;
  
  /**worker attempt id to worker attempt map*/
  private final Map<WorkerAttemptId, WorkerAttempt> attempts;
  
  /**running worker attempt id*/
  private WorkerAttemptId runningAttemptId;
  
  /**last run attempt id*/
  private WorkerAttemptId lastAttemptId;
  
  /**failed worker atttempts*/
  private final Set<WorkerAttemptId> failedAttempts;
  
  /**success worker attempt id*/
  private WorkerAttemptId successAttemptId;
  
  /**next worker attempt index*/
  private int nextAttemptNumber = 0;
  
  /**the maximum number of attempts allowed*/
  private final int maxAttempts;
  
  private final StateMachine<AMWorkerState, AMWorkerEventType, AMWorkerEvent> stateMachine;
  
  /**worker diagnostics*/
  private final List<String> diagnostics;

  public AMWorker(WorkerId id, AMContext context, List<TaskId> taskIds) {
    this.id = id;
    this.context = context;
    this.taskIds = taskIds;

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
    stateMachine = stateMachineFactory.make(this);
    metrics = new HashMap<String, String>();
    diagnostics = new ArrayList<String>();    
    attempts = new HashMap<WorkerAttemptId, WorkerAttempt>();
    failedAttempts = new HashSet<WorkerAttemptId>();
    
    maxAttempts =
        context.getConf().getInt(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS,
            AngelConf.DEFAULT_WORKER_MAX_ATTEMPTS);
  }

  private static final KillNewWorkertTransition KILL_NEW_TRANSITION =
      new KillNewWorkertTransition();

  private static final WorkerAttemptDoneTransition ATTEMPT_DONE_TRANSITION = new WorkerAttemptDoneTransition();

  private static final WorkerAssignedKilledTransition ASSIGNED_KILLED_TRANSITION =
      new WorkerAssignedKilledTransition();
  
  private static final WorkerAttemptFailedTransition ATTEMPT_FAILED_TRANSITION =
      new WorkerAttemptFailedTransition();
  
  private static final WorkerAttemptKilledTransition ATTEMPT_KILLED_TRANSITION =
      new WorkerAttemptKilledTransition();

  protected static final StateMachineFactory<AMWorker, AMWorkerState, AMWorkerEventType, AMWorkerEvent> stateMachineFactory =
      new StateMachineFactory<AMWorker, AMWorkerState, AMWorkerEventType, AMWorkerEvent>(
          AMWorkerState.NEW)                    
          .addTransition(AMWorkerState.NEW, 
              AMWorkerState.SCHEDULED, 
              AMWorkerEventType.SCHEDULE,
              new ScheduleTransation())
          .addTransition(AMWorkerState.NEW, 
              AMWorkerState.KILLED, 
              AMWorkerEventType.KILL,
              KILL_NEW_TRANSITION)

          .addTransition(
              AMWorkerState.SCHEDULED, 
              AMWorkerState.RUNNING,
              AMWorkerEventType.WORKER_ATTEMPT_REGISTED, 
              new RegisterTransition())
          .addTransition(
              AMWorkerState.SCHEDULED, 
              EnumSet.of(AMWorkerState.SCHEDULED, AMWorkerState.FAILED),
              AMWorkerEventType.WORKER_ATTEMPT_FAILED,
              ATTEMPT_FAILED_TRANSITION)
          .addTransition(
              AMWorkerState.SCHEDULED, 
              AMWorkerState.KILLED, 
              AMWorkerEventType.KILL,
              ASSIGNED_KILLED_TRANSITION)

          .addTransition(
              AMWorkerState.RUNNING, 
              AMWorkerState.SUCCESS, 
              AMWorkerEventType.WORKER_ATTEMPT_SUCCESS,
              ATTEMPT_DONE_TRANSITION)             
          .addTransition(
              AMWorkerState.RUNNING, 
              EnumSet.of(AMWorkerState.SCHEDULED, AMWorkerState.FAILED),
              AMWorkerEventType.WORKER_ATTEMPT_KILLED,
              ATTEMPT_KILLED_TRANSITION)
          .addTransition(
              AMWorkerState.RUNNING, 
              EnumSet.of(AMWorkerState.FAILED, AMWorkerState.RUNNING),
              AMWorkerEventType.WORKER_ATTEMPT_FAILED,
              ATTEMPT_FAILED_TRANSITION)
          .addTransition(
              AMWorkerState.RUNNING, 
              AMWorkerState.KILLED,
              AMWorkerEventType.KILL,
              ASSIGNED_KILLED_TRANSITION)

          .addTransition(
              AMWorkerState.KILLED,
              AMWorkerState.KILLED,
              EnumSet.of(
                  AMWorkerEventType.INIT, 
                  AMWorkerEventType.KILL, 
                  AMWorkerEventType.SCHEDULE,
                  AMWorkerEventType.WORKER_ATTEMPT_FAILED,
                  AMWorkerEventType.WORKER_ATTEMPT_KILLED,
                  AMWorkerEventType.WORKER_ATTEMPT_REGISTED,
                  AMWorkerEventType.WORKER_ATTEMPT_SUCCESS))

          .addTransition(
              AMWorkerState.FAILED,
              AMWorkerState.FAILED,
              EnumSet.of(
                  AMWorkerEventType.INIT, 
                  AMWorkerEventType.KILL, 
                  AMWorkerEventType.SCHEDULE,
                  AMWorkerEventType.WORKER_ATTEMPT_FAILED,
                  AMWorkerEventType.WORKER_ATTEMPT_KILLED,
                  AMWorkerEventType.WORKER_ATTEMPT_REGISTED,
                  AMWorkerEventType.WORKER_ATTEMPT_SUCCESS))

          .addTransition(
              AMWorkerState.SUCCESS,
              AMWorkerState.SUCCESS,
              EnumSet.of(
                  AMWorkerEventType.INIT, 
                  AMWorkerEventType.KILL, 
                  AMWorkerEventType.SCHEDULE,
                  AMWorkerEventType.WORKER_ATTEMPT_FAILED,
                  AMWorkerEventType.WORKER_ATTEMPT_KILLED,
                  AMWorkerEventType.WORKER_ATTEMPT_REGISTED,
                  AMWorkerEventType.WORKER_ATTEMPT_SUCCESS));

  static class ScheduleTransation implements SingleArcTransition<AMWorker, AMWorkerEvent> {
    @Override
    public void transition(AMWorker worker, AMWorkerEvent event) {
      LOG.info("schedule worker, workerId = " + worker.getId());
      worker.addAndScheduleAttempt();
    }
  }

  private static class KillNewWorkertTransition implements
      SingleArcTransition<AMWorker, AMWorkerEvent> {

    @Override
    public void transition(AMWorker worker, AMWorkerEvent event) {
      worker.notifyWorkerKilled();
    }
  }

  @SuppressWarnings("unchecked")
  private void addAndScheduleAttempt() {
    WorkerAttempt attempt = null;
    writeLock.lock();
    try {
      //init a worker attempt for the worker
      attempt = createWorkerAttempt();
      for(TaskId taskId:taskIds) {
        AMTask task = context.getTaskManager().getTask(taskId);
        if(task != null) {
          task.resetCounters();
        }
      }

      attempts.put(attempt.getId(), attempt);
      LOG.info("scheduling " + attempt.getId());
      runningAttemptId = attempt.getId();
      lastAttemptId = attempt.getId();
    } finally {
      writeLock.unlock();
    }

    //schedule the worker attempt
    context.getEventHandler().handle(
        new WorkerAttemptEvent(WorkerAttemptEventType.SCHEDULE, attempt.getId()));
  }

  private WorkerAttempt createWorkerAttempt() {
    WorkerAttempt attempt = null;
    if(lastAttemptId != null){
      attempt = new WorkerAttempt(id, nextAttemptNumber, context, taskIds, attempts.get(lastAttemptId));
    } else {
      attempt = new WorkerAttempt(id, nextAttemptNumber, context, taskIds, null);
    }
    
    nextAttemptNumber++;
    return attempt;
  }

  public AMContext getContext() {
    return context;
  }

  private static class WorkerAttemptFailedTransition implements
      MultipleArcTransition<AMWorker, AMWorkerEvent, AMWorkerState> {
    @Override
    public AMWorkerState transition(AMWorker worker,
        AMWorkerEvent event) {
      WorkerAttemptId workerAttemptId = ((WorkerFromAttemptEvent) event).getWorkerAttemptId();
      worker.failedAttempts.add(workerAttemptId);
      if (worker.runningAttemptId == workerAttemptId) {
        worker.runningAttemptId = null;
      }

      // add diagnostic
      StringBuilder diagnostic = new StringBuilder();
      diagnostic.append(workerAttemptId.toString()).append(" failed due to: ");
      diagnostic.append(StringUtils.join("\n", worker.attempts.get(workerAttemptId).getDiagnostics()));
      diagnostic.append(" Detail Worker Log URL:");
      diagnostic.append(worker.attempts.get(workerAttemptId).getLogUrl());
          
      if (LOG.isDebugEnabled()) {
        LOG.debug(workerAttemptId + "failed due to:" + diagnostic.toString());
      }
      worker.diagnostics.add(diagnostic.toString());

      //check whether the number of failed attempts is less than the maximum number of allowed
      if (worker.failedAttempts.size() < worker.maxAttempts) {
        //init and start a new attempt for this ps
        worker.addAndScheduleAttempt();
        return worker.stateMachine.getCurrentState();
      } else {
        //notify worker manager
        worker.notifyWorkerFailed();
        return AMWorkerState.FAILED;
      }
    }
  }
  
  private static class WorkerAttemptKilledTransition implements
      MultipleArcTransition<AMWorker, AMWorkerEvent, AMWorkerState> {

    @Override
    public AMWorkerState transition(AMWorker worker, AMWorkerEvent event) {
      WorkerAttemptId workerAttemptId = ((WorkerFromAttemptEvent) event).getWorkerAttemptId();
      worker.failedAttempts.add(workerAttemptId);
      if (worker.runningAttemptId == workerAttemptId) {
        worker.runningAttemptId = null;
      }

      // add diagnostic
      StringBuilder diagnostic = new StringBuilder();
      diagnostic.append(workerAttemptId.toString()).append(" failed due to: ");
      diagnostic.append(StringUtils.join("\n", worker.attempts.get(workerAttemptId)
          .getDiagnostics()));
      diagnostic.append(" Detail Worker Log URL:");
      diagnostic.append(worker.attempts.get(workerAttemptId).getLogUrl());

      if (LOG.isDebugEnabled()) {
        LOG.debug(workerAttemptId + "failed due to:" + diagnostic.toString());
      }
      worker.diagnostics.add(diagnostic.toString());

      // check whether the number of failed attempts is less than the maximum number of allowed
      if (worker.failedAttempts.size() < worker.maxAttempts) {
        // init and start a new attempt for this ps
        worker.addAndScheduleAttempt();
        return worker.stateMachine.getCurrentState();
      } else {
        // notify worker manager
        worker.notifyWorkerKilled();
        return AMWorkerState.KILLED;
      }
    }
  }
  
  private static class RegisterTransition implements SingleArcTransition<AMWorker, AMWorkerEvent> {
    @Override
    public void transition(AMWorker worker, AMWorkerEvent event) {
      worker.notifyWorkerRegisted();
    }
  }

  private static class WorkerAssignedKilledTransition implements
      SingleArcTransition<AMWorker, AMWorkerEvent> {

    @SuppressWarnings("unchecked")
    @Override
    public void transition(AMWorker worker, AMWorkerEvent event) {     
      StringBuilder diaggostic = new StringBuilder();
      diaggostic.append("worker is killed by user, workerId: ")
          .append(worker.getId().toString());
      worker.diagnostics.add(diaggostic.toString());
      
      for (WorkerAttempt attempt : worker.attempts.values()) {
        if (attempt != null && !attempt.isFinished()) {
          worker.context.getEventHandler()
              .handle(new WorkerAttemptEvent(WorkerAttemptEventType.KILL, attempt.getId()));
        }
      }
      
      worker.notifyWorkerKilled();
    }
  }

  private static class WorkerAttemptDoneTransition implements SingleArcTransition<AMWorker, AMWorkerEvent> {
    @Override
    public void transition(AMWorker worker, AMWorkerEvent event) {
      WorkerAttemptId attemptId = ((WorkerFromAttemptEvent) event).getWorkerAttemptId();
      worker.successAttemptId = attemptId;
      worker.runningAttemptId = null;
      worker.notifyWorkerSuccess();
    }
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerSuccess() {
    context.getEventHandler().handle(
        new WorkerGroupFromWorkerEvent(AMWorkerGroupEventType.WORKER_DONE, context
            .getWorkerManager().getWorkGroup(id).getId(), id));
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerRegisted() {
    context.getEventHandler().handle(
        new WorkerGroupFromWorkerEvent(AMWorkerGroupEventType.WORKER_REGISTED, context
            .getWorkerManager().getWorkGroup(id).getId(), id));
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerFailed() {
    StringBuilder sb = new StringBuilder();
    sb.append(id).append(" failed. ");
    int size = diagnostics.size();
    if (size == 0) {
      sb.append("No more detail message.");
    } else {
      sb.append(StringUtils.join("\n", diagnostics));
    }  

    context.getEventHandler().handle(
        new WorkerGroupDiagnosticsUpdateEvent(context.getWorkerManager().getWorkerGroup(id)
            .getId(), sb.toString()));

    context.getEventHandler().handle(
        new WorkerGroupFromWorkerEvent(AMWorkerGroupEventType.WORKER_ERROR, context.getWorkerManager()
            .getWorkerGroup(id).getId(), id));
  }

  @SuppressWarnings("unchecked")
  private void notifyWorkerKilled() {
    StringBuilder sb = new StringBuilder();
    sb.append(id).append(" killed. ");
    int size = diagnostics.size();
    if (size == 0) {
      sb.append("No more detail message.");
    } else {
      sb.append(StringUtils.join("\n", diagnostics));
    } 

    context.getEventHandler().handle(
        new WorkerGroupDiagnosticsUpdateEvent(context.getWorkerManager().getWorkerGroup(id)
            .getId(), sb.toString()));

    context.getEventHandler().handle(
        new WorkerGroupFromWorkerEvent(AMWorkerGroupEventType.WORKER_KILL, context.getWorkerManager()
            .getWorkerGroup(id).getId(), id));
  }

  public void handle(AMWorkerEvent event) {
    LOG.debug(id + " processing " + event.getWorkerId() + " of type " + event.getType());
    writeLock.lock();
    try {
      final AMWorkerState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {

      }
      if (oldState != getState()) {
        LOG.info(event.getWorkerId() + " Transitioned from " + oldState + " to "
            + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * get worker state
   * @return AMWorkerState worker state
   */
  public AMWorkerState getState() { 
    try {
      readLock.lock();
      return stateMachine.getCurrentState();
    } finally {
      readLock.unlock();
    }
  }
  
  /**
   * get worker id
   * @return WorkerId worker id
   */
  public WorkerId getId() {
    return id;
  }

  /**
   * check if the worker is running over
   * @return boolean
   */
  public boolean isFinished() {     
    try {
      readLock.lock();   
      AMWorkerState state = stateMachine.getCurrentState();
      return (state == AMWorkerState.SUCCESS || state == AMWorkerState.FAILED || state == AMWorkerState.KILLED);
    } finally {
      readLock.unlock();
    }
  }
  
  /**
   * get the running worker attempt id
   * @return WorkerAttemptId the running worker attempt id
   */
  public WorkerAttemptId getRunningAttemptId() {  
    try {
      readLock.lock();
      return runningAttemptId;
    } finally {
      readLock.unlock();
    } 
  }
  
  
  /**
   * get the running worker attempt
   * @return WorkerAttemptId the running worker attempt
   */
  public WorkerAttempt getRunningAttempt() {
    try {
      readLock.lock();
      if(runningAttemptId != null){
        return attempts.get(runningAttemptId);
      } else {
        return null;
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the success worker attempt id
   * @return WorkerAttemptId the success worker attempt id
   */
  public WorkerAttemptId getSuccessAttemptId() {
    try {
      readLock.lock();
      return successAttemptId;
    } finally {
      readLock.unlock();
    }  
  }

  /**
   * get the next worker attempt index for this worker
   * @return the next worker attempt index for this worker
   */
  public int getNextAttemptNumber() {   
    try {
      readLock.lock();
      return nextAttemptNumber;
    } finally {
      readLock.unlock();
    }  
  }

  /**
   * get the worker metrics
   * @return Map<String, String> the worker metrics
   */
  public Map<String, String> getMetrics() {  
    try {
      readLock.lock();
      Map<String, String> cloneMetrics = new HashMap<String, String>();
      cloneMetrics.putAll(metrics);
      return cloneMetrics;
    } finally {
      readLock.unlock();
    } 
  }

  /**
   * get all attempts for this worker
   * @return Map<WorkerAttemptId, WorkerAttempt> all attempts for this worker
   */
  public Map<WorkerAttemptId, WorkerAttempt> getAttempts() {
    try {
      readLock.lock();
      Map<WorkerAttemptId, WorkerAttempt> cloneAttempts = new HashMap<WorkerAttemptId, WorkerAttempt>(attempts.size());
      cloneAttempts.putAll(attempts);
      return cloneAttempts;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the failed attempt id set for this worker
   * @return Set<WorkerAttemptId> the failed attempt id set for this worker
   */
  public Set<WorkerAttemptId> getFailedAttempts() {
    try {
      readLock.lock();
      Set<WorkerAttemptId> cloneFailedAttempts = new HashSet<WorkerAttemptId>(failedAttempts.size());
      cloneFailedAttempts.addAll(failedAttempts);
      return cloneFailedAttempts;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the maximum number of attempts allowed
   * @return int the maximum number of attempts allowed
   */
  public int getMaxAttempts() {
    return maxAttempts;
  }

  /**
   * get the worker diagnostics
   * @return List<String> the worker diagnostics
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

  /**
   * get the task ids which the worker contains
   * @return List<TaskId> the task ids which the worker contains
   */
  public List<TaskId> getTaskIds() {
    return taskIds;
  }

  /**
   * get worker attempt use worker attempt id
   * @param workerAttemptId worker attempt id
   * @return WorkerAttempt worker attempt that has specified id
   */
  public WorkerAttempt getWorkerAttempt(WorkerAttemptId workerAttemptId) {
    try {
      readLock.lock();
      return attempts.get(workerAttemptId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the minimal iteration values in all the tasks contained in this worker
   * @return int the minimal iteration values in all the tasks contained in this worker
   */
  public int getMinIteration() {
    try {
      readLock.lock();
      if(runningAttemptId != null){
        return attempts.get(runningAttemptId).getMinIteration();
      } else {
        if(attempts.isEmpty()){
          return 0;
        } else {
          int minIteration = 0;
          for(WorkerAttempt workerAttempt:attempts.values()){
            if(workerAttempt.getMinIteration() > minIteration){
              minIteration = workerAttempt.getMinIteration();
            }
          }
          return minIteration;
        }
      }
    } finally {
      readLock.unlock();
    }
  }
}
