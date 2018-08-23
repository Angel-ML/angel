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


package com.tencent.angel.master.task;

import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.Pair;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskStateProto;
import com.tencent.angel.worker.task.TaskId;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Manager for a single task.
 */
public class AMTask {
  private static final Log LOG = LogFactory.getLog(AMTask.class);
  /**
   * task id
   */
  private final TaskId taskId;

  /**
   * task state
   */
  private AMTaskState state;

  /**
   * task iteration
   */
  private int iteration;

  /**
   * matrix id to clock value map
   */
  private final Int2IntOpenHashMap matrixIdToClockMap;

  /**
   * task run progress in current iteration
   */
  private float progress;

  /**
   * task metrics
   */
  private final Map<String, String> metrics;

  /**
   * task startup time
   */
  private long startTime;

  /**
   * task finish time
   */
  private long finishTime;

  private final Lock readLock;
  private final Lock writeLock;

  public AMTask(TaskId id, AMTask amTask) {
    state = AMTaskState.NEW;
    taskId = id;
    metrics = new HashMap<String, String>();
    startTime = -1;
    finishTime = -1;

    matrixIdToClockMap = new Int2IntOpenHashMap();
    // if amTask is not null, we should clone task state from it
    if (amTask == null) {
      iteration = 0;
      progress = 0.0f;
    } else {
      iteration = amTask.getIteration();
      progress = amTask.getProgress();
      matrixIdToClockMap.putAll(amTask.matrixIdToClockMap);
    }

    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    readLock = readWriteLock.readLock();
    writeLock = readWriteLock.writeLock();
  }

  /**
   * get task id
   *
   * @return TaskId task id
   */
  public TaskId getTaskId() {
    return taskId;
  }

  /**
   * get task state
   *
   * @return AMTaskState task state
   */
  public AMTaskState getState() {
    try {
      readLock.lock();
      return state;
    } finally {
      readLock.unlock();
    }
  }

  private void setState(AMTaskState newState) {
    try {
      writeLock.lock();
      switch (state) {
        case NEW: {
          state = newState;
          break;
        }
        case INITED: {
          if (newState != AMTaskState.NEW) {
            state = newState;
          }
          break;
        }
        case RUNNING:
        case WAITING: {
          if (newState != AMTaskState.NEW && newState != AMTaskState.INITED) {
            state = newState;
            break;
          }
        }
        case COMMITING: {
          if (newState == AMTaskState.SUCCESS || newState == AMTaskState.FAILED
            || newState == AMTaskState.KILLED) {
            state = newState;
          }
          break;
        }

        default: {
          break;
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * get the task progress in current iteration
   *
   * @return float the task progress in current iteration
   */
  public float getProgress() {
    try {
      readLock.lock();
      return progress;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * update task information: iteration, clocks, counters, progress, startup time and finish time etc.
   *
   * @param taskStateProto task information from worker
   */
  public void updateTaskState(TaskStateProto taskStateProto) {
    try {
      writeLock.lock();
      LOG.debug("taskStateProto=" + taskStateProto);
      setState(transformState(taskStateProto.getState()));
      this.progress = taskStateProto.getProgress();
      if (taskStateProto.hasIteration()) {
        this.iteration = taskStateProto.getIteration();
      }
      updateMatrixClocks(taskStateProto.getMatrixClocksList());
      updateMetrics(taskStateProto.getCountersList());

      if (taskStateProto.hasFinishTime()) {
        this.finishTime = taskStateProto.getFinishTime();
      }
      if (taskStateProto.hasStartTime()) {
        this.startTime = taskStateProto.getStartTime();
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void updateCounters(List<Pair> counters) {
    try {
      writeLock.lock();
      updateMetrics(counters);
    } finally {
      writeLock.unlock();
    }
  }

  private void updateMetrics(List<Pair> counters) {
    int size = counters.size();
    for (int i = 0; i < size; i++) {
      metrics.put(counters.get(i).getKey(), counters.get(i).getValue());
    }
  }

  private void updateMatrixClocks(List<MatrixClock> matrixClocks) {
    int size = matrixClocks.size();
    for (int i = 0; i < size; i++) {
      matrixIdToClockMap.put(matrixClocks.get(i).getMatrixId(), matrixClocks.get(i).getClock());
    }
  }

  private AMTaskState transformState(String state) {
    switch (state) {
      case "NEW":
        return AMTaskState.NEW;
      case "INITED":
        return AMTaskState.INITED;
      case "RUNNING":
        return AMTaskState.RUNNING;
      case "WAITING":
        return AMTaskState.WAITING;
      case "COMMITING":
        return AMTaskState.COMMITING;
      case "SUCCESS":
        return AMTaskState.SUCCESS;
      case "FAILED":
        return AMTaskState.FAILED;
      case "KILLED":
        return AMTaskState.KILLED;
      default:
        return AMTaskState.NEW;
    }
  }

  /**
   * get task startup time
   *
   * @return long task startup time
   */
  public long getStartTime() {
    try {
      readLock.lock();
      return startTime;
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get task finish time
   *
   * @return long task finish time
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
   * get the current iteration value
   *
   * @return int the current iteration value
   */
  public int getIteration() {
    try {
      readLock.lock();
      return iteration;
    } finally {
      readLock.unlock();
    }
  }

  @Override public String toString() {
    try {
      readLock.lock();
      return "AMTask [taskId=" + taskId + ", state=" + state + ", iteration=" + iteration
        + ", matrixIdToClockMap=" + matrixIdToClockMap.toString() + ", progress=" + progress + "]";
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get task metrics
   *
   * @return Map<String, String> task metrics
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
   * update the clock value of the specified matrix
   *
   * @param matrixId matrix id
   * @param clock    clock value
   */
  public void clock(int matrixId, int clock) {
    try {
      writeLock.lock();
      matrixIdToClockMap.put(matrixId, clock);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * update the task current iteration value
   *
   * @param iteration the task current iteration value
   */
  public void iteration(int iteration) {
    try {
      writeLock.lock();
      this.iteration = iteration;
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * get all matrix clocks
   *
   * @return Int2IntOpenHashMap  all matrix clocks
   */
  public Int2IntOpenHashMap getMatrixClocks() {
    try {
      readLock.lock();
      return matrixIdToClockMap.clone();
    } finally {
      readLock.unlock();
    }
  }

  /**
   * get the clock of the specified matrix
   *
   * @param matrixId the matrix id
   * @return int the clock of the matrix
   */
  public int getMatrixClock(int matrixId) {
    try {
      readLock.lock();
      return matrixIdToClockMap.get(matrixId);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * write the task state to a output stream
   *
   * @param output the output stream
   */
  public void serialize(DataOutputStream output) throws IOException {
    try {
      readLock.lock();
      output.writeUTF(state.toString());
      output.writeInt(iteration);
      output.writeInt(matrixIdToClockMap.size());
      for (Entry clockEntry : matrixIdToClockMap.int2IntEntrySet()) {
        output.writeInt(clockEntry.getIntKey());
        output.writeInt(clockEntry.getIntValue());
      }
    } finally {
      readLock.unlock();
    }
  }

  /**
   * read the task state from a input stream
   *
   * @param input the input stream
   */
  public void deserialize(DataInputStream input) throws IOException {
    try {
      writeLock.lock();
      state = transformState(input.readUTF());
      iteration = input.readInt();
      int clockNum = input.readInt();
      for (int i = 0; i < clockNum; i++) {
        matrixIdToClockMap.put(input.readInt(), input.readInt());
      }
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Reset some profiling counters
   */
  public void resetCounters() {
    metrics.put(TaskCounter.TOTAL_CALCULATE_SAMPLES, "0");
    metrics.put(TaskCounter.TOTAL_CALCULATE_TIME_MS, "0");
  }
}
