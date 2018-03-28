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

package com.tencent.angel.master.worker;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.app.AppEvent;
import com.tencent.angel.master.app.AppEventType;
import com.tencent.angel.master.app.InternalErrorEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptDiagnosticsUpdateEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEvent;
import com.tencent.angel.master.worker.attempt.WorkerAttemptEventType;
import com.tencent.angel.master.worker.worker.AMWorker;
import com.tencent.angel.master.worker.worker.AMWorkerEvent;
import com.tencent.angel.master.worker.worker.AMWorkerEventType;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroup;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupEvent;
import com.tencent.angel.master.worker.workergroup.AMWorkerGroupEventType;
import com.tencent.angel.utils.StringUtils;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;
import com.tencent.angel.worker.task.TaskId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Global worker manager, it manages all worker groups {@link com.tencent.angel.master.worker.workergroup.AMWorkerGroup}
 * and all workers {@link com.tencent.angel.master.worker.worker.AMWorker} in the application.
 */
public class WorkerManager implements EventHandler<WorkerManagerEvent> {
  private static final Log LOG = LogFactory.getLog(WorkerManager.class);

  private final AMContext context;
  
  /**the amount of resources requested for each worker*/
  private final Resource workerResource;
  
  /**the resource priority for worker*/
  private final Priority PRIORITY_WORKER;

  /**worker number in a worker group*/
  private final int workersInGroup;
  
  /**task number in each worker*/
  private final int taskNumberInEachWorker;
  
  /**tolerate of the failure worker group ratio*/
  private final double tolerateFailedGroup;
  
  /**actual worker group number*/
  private volatile int workergroupNumber;
  
  /**actual total task number in application*/
  private volatile int totalTaskNumber;

  /**worker group id to worker group map*/
  private final Map<WorkerGroupId, AMWorkerGroup> workerGroupMap;
  
  /**worker id to the worker group which the worker belongs to map*/
  private final Map<WorkerId, AMWorkerGroup> findWorkerGroupMap;
  
  /**worker id to worker map*/
  private final Map<WorkerId, AMWorker> workersMap;
  
  /**task id to the worker which the task belongs to map*/
  private final Map<TaskId, AMWorker> taskIdToWorkerMap;
  
  /**success worker group id set*/
  private final Set<WorkerGroupId> successGroups;
  
  /**killed worker group id set*/
  private final Set<WorkerGroupId> killedGroups;
  
  /**failed worker group id set*/
  private final Set<WorkerGroupId> failedGroups;

  /**worker attempt id to last heartbeat timestamp map*/
  private final ConcurrentHashMap<WorkerAttemptId, Long> workerLastHeartbeatTS = new ConcurrentHashMap<>();
  /**worker timeout value in millisecond*/
  private final long workerTimeOutMS;
  
  private final Lock readLock;
  private final Lock writeLock;
  
  private boolean isInited = false;

  public WorkerManager(AMContext context) {
    this.context = context;
    
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();

    Configuration conf = context.getConf();
    workersInGroup =
        conf.getInt(AngelConf.ANGEL_WORKERGROUP_WORKER_NUMBER,
            AngelConf.DEFAULT_ANGEL_WORKERGROUP_WORKER_NUMBER);

    taskNumberInEachWorker =
        conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
            AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER);

    tolerateFailedGroup =
        conf.getDouble(AngelConf.ANGEL_WORKERGROUP_FAILED_TOLERATE, conf.getDouble(
            AngelConf.ANGEL_TASK_ERROR_TOLERATE,
            AngelConf.DEFAULT_ANGEL_TASK_ERROR_TOLERATE));

    int workerMemory =
        conf.getInt(AngelConf.ANGEL_WORKER_MEMORY_GB,
            AngelConf.DEFAULT_ANGEL_WORKER_MEMORY_GB) * 1024;
    int workerVcores =
        conf.getInt(AngelConf.ANGEL_WORKER_CPU_VCORES,
            AngelConf.DEFAULT_ANGEL_WORKER_CPU_VCORES);

    int workerPriority =
        conf.getInt(AngelConf.ANGEL_WORKER_PRIORITY,
            AngelConf.DEFAULT_ANGEL_WORKER_PRIORITY);

    workerTimeOutMS =
      conf.getLong(AngelConf.ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS,
        AngelConf.DEFAULT_ANGEL_WORKER_HEARTBEAT_TIMEOUT_MS);

    workerResource = Resource.newInstance(workerMemory, workerVcores);
    PRIORITY_WORKER =
        RecordFactoryProvider.getRecordFactory(null).newRecordInstance(Priority.class);
    PRIORITY_WORKER.setPriority(workerPriority);

    workerGroupMap = new HashMap<WorkerGroupId, AMWorkerGroup>();
    findWorkerGroupMap = new HashMap<WorkerId, AMWorkerGroup>();
    workersMap = new HashMap<WorkerId, AMWorker>();
    taskIdToWorkerMap = new HashMap<TaskId, AMWorker>();
    successGroups = new HashSet<WorkerGroupId>();
    killedGroups = new HashSet<WorkerGroupId>();
    failedGroups = new HashSet<WorkerGroupId>();
  }

  public AMWorkerGroup getWorkGroup(WorkerId workerId) {
    try{
      readLock.lock();
      return findWorkerGroupMap.get(workerId);
    } finally {
      readLock.unlock();
    } 
  }

  public Map<WorkerGroupId, AMWorkerGroup> getWorkerGroupMap() {
    try{
      readLock.lock();
      Map<WorkerGroupId, AMWorkerGroup> clonedMap = new HashMap<WorkerGroupId, AMWorkerGroup>(workerGroupMap.size());
      clonedMap.putAll(workerGroupMap);
      return clonedMap;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void handle(WorkerManagerEvent event) {
    try{
      writeLock.lock();
      handleEvent(event);
    } finally {
      writeLock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  private void handleEvent(WorkerManagerEvent event){
    switch (event.getType()) {
      case WORKERGROUP_DONE: {
        WorkerGroupManagerEvent workerGroupEvent = (WorkerGroupManagerEvent) event;
        //add this worker group to the success set
        successGroups.add(workerGroupEvent.getWorkerGroupId());
        
        //check if all worker group run over
        if (checkISAllGroupEnd()) {
          LOG.info("now all WorkerGroups are finished!");         
          context.getEventHandler().handle(new AppEvent(AppEventType.EXECUTE_SUCESS));
        }
        break;
      }

      case WORKERGROUP_FAILED: {
        WorkerGroupManagerEvent workerGroupEvent = (WorkerGroupManagerEvent) event;
        //add this worker group to the failed set
        failedGroups.add(workerGroupEvent.getWorkerGroupId());
        
        //check if too many worker groups are failed or killed
        if (checkISOverTolerate()) {
          //notify a run failed event
          context.getEventHandler().handle(new InternalErrorEvent(context.getApplicationId(), getDetailWorkerExitMessage()));
        }
        break;
      }

      case WORKERGROUP_KILLED: {
        WorkerGroupManagerEvent workerGroupEvent = (WorkerGroupManagerEvent) event;
        //add this worker group to the failed set
        killedGroups.add(workerGroupEvent.getWorkerGroupId());
        
        //check if too many worker groups are failed or killed
        if (checkISOverTolerate()) {
          //notify a run failed event
          context.getEventHandler().handle(new InternalErrorEvent(context.getApplicationId(), getDetailWorkerExitMessage()));
        }
        break;
      }

      default:
        break;
    }
  }

  private String getDetailWorkerExitMessage() {
    StringBuilder sb = new StringBuilder();
    sb.append("killed and failed workergroup is over tolerate ").append(tolerateFailedGroup);
    sb.append("There are some Workers failed\n");
    if (!failedGroups.isEmpty()) {
      sb.append("failed workergroups:");
      for (WorkerGroupId groupId : failedGroups) {
        sb.append("\n");
        sb.append(groupId);
        sb.append(". ");
        sb.append(StringUtils.join("\n", workerGroupMap.get(groupId).getDiagnostics()));
      }
      sb.append("\n");
    }

    if (!killedGroups.isEmpty()) {
      sb.append("killed workergroups:");
      for (WorkerGroupId groupId : killedGroups) {
        sb.append("\n");
        sb.append(groupId);
        sb.append(". ");
        sb.append(StringUtils.join("\n", workerGroupMap.get(groupId).getDiagnostics()));
      }
      sb.append("\n");
    }
    return sb.toString();
  }

  /**
   * init and start all workers
   */
  public void startAllWorker() {
    LOG.info("to start all workers.....");
    try{
      writeLock.lock();
      initWorkers();
      for (int i = 0; i < workerGroupMap.size(); i++) {
        AMWorkerGroup group = workerGroupMap.get(new WorkerGroupId(i));
        for (AMWorker worker : group.getWorkerSet()) {
          worker.handle(new AMWorkerEvent(AMWorkerEventType.SCHEDULE, worker.getId()));
        }
      }
      
      isInited = true;
    } finally {
      writeLock.unlock();
    }
  }
  
  public void adjustTaskNumber(int splitNum) {
    //calculate the actual number of worker groups and the total number of tasks based on the number of data split
    int estimatedGroupNum = (splitNum + taskNumberInEachWorker - 1) / taskNumberInEachWorker;
    int estimatedTaskNum = splitNum * workersInGroup;

    workergroupNumber = estimatedGroupNum;
    totalTaskNumber = estimatedTaskNum;
    context.getConf().setInt(AngelConf.ANGEL_TASK_ACTUAL_NUM, totalTaskNumber);
    context.getConf().setInt(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM, workergroupNumber);
  }

  private void initWorkers() {
    int base = 0;
    //init all tasks , workers and worker groups and put them to the corresponding maps 
    for (int i = 0; i < workergroupNumber; i++) {
      Map<WorkerId, AMWorker> workers = new HashMap<WorkerId, AMWorker>();
      WorkerId leader = null;
      WorkerGroupId groupId = new WorkerGroupId(i);

      for (int j = 0; j < workersInGroup; j++) {
        base = (i * workersInGroup + j) * taskNumberInEachWorker;
        List<TaskId> taskIds = new ArrayList<TaskId>(taskNumberInEachWorker);
        for (int k = 0; k < taskNumberInEachWorker && (base < totalTaskNumber); k++, base++) {
          taskIds.add(new TaskId(base));
        }

        WorkerId workerId = new WorkerId(groupId, i * workersInGroup + j);
        AMWorker worker = new AMWorker(workerId, context, taskIds);
        workersMap.put(workerId, worker);
        workers.put(workerId, worker);

        if (j == 0) {
          leader = workerId;
        }
      }

      AMWorkerGroup group = new AMWorkerGroup(groupId, context, workers, leader, i);
      for (WorkerId id : workers.keySet()) {
        findWorkerGroupMap.put(id, group);
        for(TaskId taskId:workers.get(id).getTaskIds()){
          taskIdToWorkerMap.put(taskId, workers.get(id));
        }
      }

      workerGroupMap.put(groupId, group);
      group.handle(new AMWorkerGroupEvent(AMWorkerGroupEventType.INIT, groupId));
    }
    LOG.info("to init taskClockManager!");
  }

  private boolean checkISOverTolerate() {
    return tolerateFailedGroup <= (double) (failedGroups.size() + killedGroups.size())
        / workergroupNumber;
  }

  private boolean checkISAllGroupEnd() {
    return workergroupNumber <= (successGroups.size() + failedGroups.size() + killedGroups.size());
  }

  /**
   * get worker use worker id
   * @param workerId worker id
   * @return AMWorker worker
   */
  public AMWorker getWorker(WorkerId workerId) {
    try{
      readLock.lock();
      return workersMap.get(workerId);
    } finally {
      readLock.unlock();
    }    
  }

  /**
   * get worker group use worker group id
   * @param workerGroupId worker group id
   * @return AMWorkerGroup worker group
   */
  public AMWorkerGroup getWorkerGroup(WorkerGroupId workerGroupId) {
    try{
      readLock.lock();
      return workerGroupMap.get(workerGroupId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the worker group which contains the specified worker id
   * @param workerId worker id
   * @return AMWorkerGroup the worker group which contains the specified worker id
   */
  public AMWorkerGroup getWorkerGroup(WorkerId workerId) {
    try{
      readLock.lock();
      return findWorkerGroupMap.get(workerId);
    } finally {
      readLock.unlock();
    }   
  }

  /**
   * get actual total task number
   * @return int actual total task number
   */
  public int getTotalTaskNumber() {
    try{
      readLock.lock();
      return totalTaskNumber;
    } finally {
      readLock.unlock();
    } 
  }

  /**
   * get actual worker group number
   * @return int actual worker group number
   */
  public int getWorkerGroupNumber() {
    try{
      readLock.lock();
      return workergroupNumber;
    } finally {
      readLock.unlock();
    } 
  }

  /**
   * get worker number
   * @return int worker number
   */
  public int getWorkerNumber() {
    try{
      readLock.lock();
      return workersMap.size();
    } finally {
      readLock.unlock();
    }    
  }
  
  /**
   * get the number of worker groups that are not finish
   * @return int the number of worker groups that are not finish
   */
  public int getActiveWorkerGroupNumber() {
    try{
      readLock.lock();
      int number = 0;
      for (Entry<WorkerGroupId, AMWorkerGroup> entry : workerGroupMap.entrySet()) {
        if (!entry.getValue().isFinished()) {
          number++;
        }
      }
      return number;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the number of workers that are not finish
   * @return int the number of workers that are not finish
   */
  public int getActiveWorkerNumber() {
    try{
      readLock.lock();
      int number = 0;
      for (Entry<WorkerId, AMWorker> entry : workersMap.entrySet()) {
        if (!entry.getValue().isFinished()) {
          number++;
        }
      }
      return number;
    } finally {
      readLock.unlock();
    }
  }
  
  /**
   * get the active task number
   * @return int the active task number 
   */
  public int getActiveTaskNum() {
    try{
      readLock.lock();
      //just return the total task number now
      //TODO
      return totalTaskNumber;
    } finally {
      readLock.unlock();
    } 
  }

  /**
   * get the worker resource quota
   * @return Resource the worker resource quota
   */
  public Resource getWorkerResource() {
    return workerResource;
  }

  /**
   * get the worker resource priority
   * @return Resource the worker resource priority
   */
  public Priority getWorkerPriority() {
    return PRIORITY_WORKER;
  }
  
  /**
   * get the iteration number of the slowest worker
   * @return int the iteration number of the slowest worker
   */
  public int getMinIteration() {
    int minIteration = Integer.MAX_VALUE;
    try{
      readLock.lock();
      if(!isInited) {
        return 0;
      }
      
      for(AMWorkerGroup group:workerGroupMap.values()){
        int groupMinIteration = group.getMinIteration();
        if(groupMinIteration < minIteration){
          minIteration = groupMinIteration;
        }
      }
      return minIteration;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the worker which contains specified task
   * @return AMWorker the worker which contains specified task
   */
  public AMWorker getWorker(TaskId taskId) {
    return taskIdToWorkerMap.get(taskId);
  }

  public void checkHBTimeOut() {
    //check whether worker heartbeat timeout
    Iterator<Map.Entry<WorkerAttemptId, Long>> workerIt = workerLastHeartbeatTS.entrySet().iterator();
    long currentTs = System.currentTimeMillis();
    while (workerIt.hasNext()) {
      Entry<WorkerAttemptId, Long> workerEntry = workerIt.next();
      if (currentTs - workerEntry.getValue() > workerTimeOutMS) {
        LOG.error(workerEntry.getKey() + " heartbeat timeout!!!");
        context.getEventHandler().handle(
          new WorkerAttemptDiagnosticsUpdateEvent(workerEntry.getKey(), "heartbeat timeout"));

        context.getEventHandler().handle(
          new WorkerAttemptEvent(WorkerAttemptEventType.ERROR, workerEntry.getKey()));
        workerIt.remove();
      }
    }
  }

  /**
   * remove worker attempt from monitor set
   * @param workerAttemptId worker attempt id
   */
  public void unRegister(WorkerAttemptId workerAttemptId) {
    LOG.info(workerAttemptId + " is unregistered in monitor!");
    workerLastHeartbeatTS.remove(workerAttemptId);
  }

  /**
   * add worker attempt to monitor set
   * @param workerAttemptId worker attempt id
   */
  public void register(WorkerAttemptId workerAttemptId) {
    LOG.info(workerAttemptId + " is registered in monitor!");
    workerLastHeartbeatTS.put(workerAttemptId, System.currentTimeMillis());
  }

  /**
   * Check is a Worker alive
   * @param workerAttemptId Worker attempt id
   * @return true mean alive
   */
  public boolean isAlive(WorkerAttemptId workerAttemptId) {
    return workerLastHeartbeatTS.containsKey(workerAttemptId);
  }

  /**
   * Update Worker lastest heartbeat timestamp
   * @param workerAttemptId Worker attempt id
   */
  public void alive(WorkerAttemptId workerAttemptId) {
    workerLastHeartbeatTS.put(workerAttemptId, System.currentTimeMillis());
  }

  /**
   * Check is there worker group run success
   * @return true means some workergroups have run success
   */
  public boolean isThereWorkerGroupSuccess() {
    return !successGroups.isEmpty();
  }

  /**
   * Get success worker group number
   * @return success worker group number
   */
  public int getSuccessWorkerGroupNum() {
    return successGroups.size();
  }
}
