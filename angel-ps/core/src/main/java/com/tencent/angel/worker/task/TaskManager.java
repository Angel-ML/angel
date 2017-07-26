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

package com.tencent.angel.worker.task;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.worker.WorkerContext;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Manages tasks running,include state monitor and task's indexes update
 */
public class TaskManager {
  private static final Log LOG = LogFactory.getLog(TaskManager.class);
  private final Map<TaskId, Task> runningTask;
  private volatile ExecutorService taskPool;

  public TaskManager() {
    runningTask = new HashMap<TaskId, Task>();
  }

  /**
   * Init.
   */
  public void init() {
    Configuration conf = WorkerContext.get().getConf();
    int taskNumInWork =
        conf.getInt(AngelConf.ANGEL_WORKER_TASK_NUMBER,
            AngelConf.DEFAULT_ANGEL_WORKER_TASK_NUMBER);
    taskPool = Executors.newFixedThreadPool(taskNumInWork);
  }

  /**
   * Find task by task id.
   *
   * @param id the task id
   * @return the task
   */
  public Task findTask(TaskId id) {
    return runningTask.get(id);
  }

  /**
   * Find task context by task id
   *
   * @param id the task id
   * @return the task context
   */
  public TaskContext findTaskContext(TaskId id) {
    return runningTask.get(id).getTaskContext();
  }

  /**
   * Gets running task.
   *
   * @return the running task
   */
  public Map<TaskId, Task> getRunningTask() {
    return runningTask;
  }

  public void stop() {
    if (taskPool != null) {
      taskPool.shutdownNow();
    }
  }

  /**
   * Start all tasks form task contexts
   *
   * @param taskIdToContextMap task contexts
   */
  public void startAllTasks(Map<TaskId, TaskContext> taskIdToContextMap) {
    LOG.info("start all tasks");
    WorkerContext.get().getDataBlockManager().assignSplitToTasks(taskIdToContextMap.keySet());
    for (Entry<TaskId, TaskContext> entry : taskIdToContextMap.entrySet()) {
      LOG.info("start task " + entry.getKey() + " with context=" + entry.getValue());
      Task task = new Task(entry.getKey(), entry.getValue());
      runningTask.put(entry.getKey(), task);
      taskPool.execute(task);
    }
  }

  /**
   * Gets task count.
   *
   * @return the task count
   */
  public int getTaskCount() {
    return runningTask.size();
  }

  /**
   * Is all task success.
   *
   * @return true if all tasks success else false
   */
  public boolean isAllTaskSuccess() {
    for (Entry<TaskId, Task> entry : runningTask.entrySet()) {
      if (!(entry.getValue().getTaskState() == TaskState.SUCCESS)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Is all task running.
   *
   * @return true if all tasks running else false
   */
  public boolean isAllTaskRunning() {
    for (Entry<TaskId, Task> entry : runningTask.entrySet()) {
      if (!(entry.getValue().getTaskState() == TaskState.RUNNING)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Is all tasks state is final(exit execution).
   *
   * @return true if all tasks state is final else false
   */
  public boolean isAllTaskExit() {
    for (Entry<TaskId, Task> entry : runningTask.entrySet()) {
      if (!(entry.getValue().getTaskState() == TaskState.SUCCESS)
          && !(entry.getValue().getTaskState() == TaskState.FAILED)
          && !(entry.getValue().getTaskState() == TaskState.KILLED)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets diagnostics.
   *
   * @return the diagnostics
   */
  public String getDiagnostics() {
    StringBuilder sb = new StringBuilder();
    for (Entry<TaskId, Task> entry : runningTask.entrySet()) {
      if (entry.getValue().getTaskState() != TaskState.SUCCESS) {
        sb.append(entry.getValue().getDiagnostics());
      }
    }

    return sb.toString();
  }

  /**
   * Combine update index.
   */
  @SuppressWarnings("rawtypes")
  public void combineUpdateIndex() {
    IntOpenHashSet indexSet = null;
    MatrixMeta meta = null;
    for (Entry<TaskId, Task> entry : runningTask.entrySet()) {
      LabeledUpdateIndexBaseTask task = (LabeledUpdateIndexBaseTask) entry.getValue().getUserTask();
      IntOpenHashSet taskIndexSet = task.getIndexSet();
      if (taskIndexSet != null) {
        if (indexSet == null) {
          indexSet = taskIndexSet;
          meta = task.getMatrixMeta();
        } else {
          indexSet.addAll(taskIndexSet);
          task.setIndexSet(null);
        }
      }
    }

    if (indexSet != null) {
      int size = indexSet.size();
      int[] indexArray = new int[size];
      int index = 0;
      IntIterator iter = indexSet.iterator();
      while (iter.hasNext()) {
        indexArray[index++] = iter.nextInt();
      }

      Arrays.sort(indexArray);

      List<PartitionKey> partKeyList =
          WorkerContext.get().getPSAgent().getMatrixPartitionRouter()
              .getPartitionKeyList(meta.getId());
      Collections.sort(partKeyList);
      int partNum = partKeyList.size();
      int lastPos = 0;
      for (int i = 0; i < partNum; i++) {
        PartitionKey partKey = partKeyList.get(i);
        int endCol = partKey.getEndCol();
        for (int j = lastPos; j < size; j++) {
          if (indexArray[j] >= endCol) {

            lastPos = j;
            break;
          }
        }
      }
      // Bitmap bitmap = new Bitmap();
      // int max = indexArray[size - 1];
      // byte [] bitIndexArray = new byte[max / 8 + 1];
      // for(int i = 0; i < size; i++){
      // int bitIndex = indexArray[i] >> 3;
      // int bitOffset = indexArray[i] - (bitIndex << 3);
      // switch(bitOffset){
      // case 0:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x01);break;
      // case 1:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x02);break;
      // case 2:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x04);break;
      // case 3:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x08);break;
      // case 4:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x10);break;
      // case 5:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x20);break;
      // case 6:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x40);break;
      // case 7:bitIndexArray[bitIndex] = (byte) (bitIndexArray[bitIndex] & 0x80);break;
      // }
      // }
    }
  }
}
