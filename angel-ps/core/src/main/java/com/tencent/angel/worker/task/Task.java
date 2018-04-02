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

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.worker.WorkerContext;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Task is the calculation unit of Angel Application, run on {@link com.tencent.angel.worker.Worker} at background as Thread.
 * And will reflect to invoke user special task which inherited by {@link BaseTask}({@link AngelConf#ANGEL_TASK_USER_TASKCLASS})
 *
 */
public class Task extends Thread {
  private static final Log LOG = LogFactory.getLog(Task.class);
  private Class<?> userTaskClass;
  private final TaskId taskId;
  private volatile TaskState state;
  final List<String> diagnostics;

  @SuppressWarnings("rawtypes")
  private volatile BaseTask userTask;

  final TaskContext taskContext;

  public Task(TaskId taskId, TaskContext taskContext) {
    this.taskId = taskId;
    this.taskContext = taskContext;
    this.diagnostics = new ArrayList<String>();
    setState(TaskState.NEW);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void run() {
    Configuration conf = WorkerContext.get().getConf();

    LOG.info("task " + taskId + " is running.");
    try {
      userTaskClass =
          conf.getClassByName(conf.get(AngelConf.ANGEL_TASK_USER_TASKCLASS,
              AngelConf.DEFAULT_ANGEL_TASK_USER_TASKCLASS));
      LOG.info("userTaskClass = " + userTaskClass + " task index = " + taskContext.getTaskIndex() + ", name = " + this.getName());

      BaseTask userTask = newBaseTask(userTaskClass);
      this.userTask =  userTask;
      runUser(userTask);
    } catch (Throwable e) {
      LOG.error("task runner error ", e);
      diagnostics.add("task runner error: " + ExceptionUtils.getFullStackTrace(e));
      setState(TaskState.FAILED);
    }

    taskExit();
  }

  @SuppressWarnings("rawtypes")
  private BaseTask newBaseTask(Class<?> userTask) throws NoSuchMethodException, SecurityException,
      InstantiationException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException {
      Constructor<?> meth = userTask.getDeclaredConstructor(TaskContext.class);
      meth.setAccessible(true);
      return (BaseTask) meth.newInstance(taskContext);
  }

  private <KEY, VALUE, VALUEOUT> void runUser(BaseTask<KEY, VALUE, VALUEOUT> userTask)
      throws Exception {
    setState(TaskState.PREPROCESSING);
    userTask.preProcess(taskContext);
    setState(TaskState.RUNNING);

    userTask.run(taskContext);
    setState(TaskState.COMMITING);
    commit();
    setState(TaskState.SUCCESS);
  }

  @SuppressWarnings("unused")
  private <KEY, VALUE> void combineUpdateIndex() {
    TaskManager manager = WorkerContext.get().getTaskManager();
    if (manager.isAllTaskRunning()) {
      manager.combineUpdateIndex();
    }
  }

  private void taskExit() {
    TaskManager manager = WorkerContext.get().getTaskManager();
    if ((manager != null) && manager.isAllTaskExit()) {
      if (manager.isAllTaskSuccess()) {
        WorkerContext.get().getWorker().workerDone();
      } else {
        WorkerContext.get().getWorker().workerError(manager.getDiagnostics());
      }
    }
  }

  private void commit() {}

  /**
   * Gets task context.
   *
   * @return the task context
   */
  public TaskContext getTaskContext() {
    return taskContext;
  }

  /**
   * Gets current task state.
   *
   * @return the task state
   */
  public TaskState getTaskState() {
    return state;
  }

  /**
   * Sets state.
   *
   * @param state the state
   */
  public void setState(TaskState state) {
    this.state = state;
  }

  /**
   * Gets progress.
   *
   * @return the progress
   */
  public float getProgress() {
    return taskContext.getProgress();
  }

  /**
   * Gets diagnostics.
   *
   * @return the diagnostics
   */
  public String getDiagnostics() {
    return "taskid=" + taskId + ", state=" + state + ", diagnostics="
        + Arrays.toString(diagnostics.toArray(new String[0]));
  }

  /**
   * Gets user task.
   *
   * @return the user task
   */
  @SuppressWarnings("rawtypes")
  public BaseTask getUserTask() {
    return userTask;
  }

}
