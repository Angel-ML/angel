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

package com.tencent.angel.psagent.task;

import com.google.protobuf.ServiceException;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.task.TaskCounter;
import com.tencent.angel.ml.metrics.Metric;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.index.MatrixIndex;
import com.tencent.angel.psagent.matrix.storage.MatrixStorageManager;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Task running context. Task is executing unit for consistency control.
 */
public class TaskContext {

  // matrix id to clock map
  private final ConcurrentHashMap<Integer, AtomicInteger> matrixIdToClockMap;

  // task index, it must be unique for whole application
  private final int index;

  private final ConcurrentHashMap<Integer, MatrixIndex> matrixIndexes;

  /** Matrix storage for task */
  private final MatrixStorageManager matrixStorage;

  /** Current iteration number */
  private final AtomicInteger iteration;

  /** Update the matrix clock to Master synchronously */
  private final boolean syncClockEnable;

  /** Task current iteration running progress */
  private volatile float progress;

  /** Task system metrics */
  private final Map<String, AtomicLong> metrics;

  /** Task algorithm metrics*/
  private final Map<String, Metric> algoMetrics;

  /**
   * Create a TaskContext
   * @param index task index
   */
  public TaskContext(int index) {
    this.index = index;
    this.matrixIdToClockMap = new ConcurrentHashMap<Integer, AtomicInteger>();
    this.matrixStorage = new MatrixStorageManager();
    this.iteration = new AtomicInteger(0);
    this.matrixIndexes = new ConcurrentHashMap<>();
    this.metrics = new ConcurrentHashMap<>();
    this.algoMetrics = new ConcurrentHashMap<>();

    syncClockEnable =
        PSAgentContext
            .get()
            .getConf()
            .getBoolean(AngelConf.ANGEL_PSAGENT_SYNC_CLOCK_ENABLE,
                AngelConf.DEFAULT_ANGEL_PSAGENT_SYNC_CLOCK_ENABLE);
  }

  /**
   * Get task index
   * @return task index
   */
  public int getIndex() {
    return index;
  }

  /**
   * Get the clock value of a matrix
   * @param matrixId matrix id
   * @return clock value
   */
  public int getMatrixClock(int matrixId) {
    if (!matrixIdToClockMap.containsKey(matrixId)) {
      matrixIdToClockMap.putIfAbsent(matrixId, new AtomicInteger(0));
    }
    return matrixIdToClockMap.get(matrixId).get();
  }

  public MatrixIndex getMatrixIndex(int matrixId) {
    return matrixIndexes.get(matrixId);
  }

  @Override
  public String toString() {
    return "TaskContext [index=" + index + ", matrix clocks=" + printMatrixClocks() + "]";
  }

  private String printMatrixClocks() {
    StringBuilder sb = new StringBuilder();

    for (Entry<Integer, AtomicInteger> entry : matrixIdToClockMap.entrySet()) {
      sb.append("(matrixId=");
      sb.append(entry.getKey());
      sb.append(",");
      sb.append("clock=");
      sb.append(entry.getValue().get());
      sb.append(")");
    }

    return sb.toString();
  }

  /**
   * Increase the clock value of the matrix
   * @param matrixId matrix id
   */
  public void increaseMatrixClock(int matrixId) {
    if (!matrixIdToClockMap.containsKey(matrixId)) {
      matrixIdToClockMap.putIfAbsent(matrixId, new AtomicInteger(0));
    }
    matrixIdToClockMap.get(matrixId).incrementAndGet();
  }

  /**
   * Get the matrix storage manager for this task
   * @return matrix storage manager
   */
  public MatrixStorageManager getMatrixStorage() {
    return matrixStorage;
  }

  /**
   * Get Task current iteration number
   * @return current iteration number
   */
  public int getIteration() {
    return iteration.get();
  }

  /**
   * Increase iteration number
   * @throws ServiceException
   */
  public void increaseIteration() throws ServiceException {
    int iterationValue = iteration.incrementAndGet();
    if(syncClockEnable){
      PSAgentContext.get().getMasterClient().setAlgoMetrics(index, algoMetrics);
      PSAgentContext.get().getMasterClient().taskIteration(index, iterationValue);
    }
  }

  /**
   * Set the iteration number
   * @param iterationValue iteration number
   */
  public void setIteration(int iterationValue){
    iteration.set(iterationValue);
  }

  /**
   * Set matrix clock value
   * @param matrixId matrix id
   * @param clockValue clock value
   */
  public void setMatrixClock(int matrixId, int clockValue){
    AtomicInteger clock = matrixIdToClockMap.get(matrixId);
    if(clock == null){
      clock = matrixIdToClockMap.putIfAbsent(matrixId, new AtomicInteger(clockValue));
      if(clock == null){
        clock = matrixIdToClockMap.get(matrixId);
      }
    }
    
    clock.set(clockValue);
  }

  /**
   * Get matrix clocks
   * @return matrix clocks
   */
  public Map<Integer, AtomicInteger> getMatrixClocks() {
    return matrixIdToClockMap;
  }

  /**
   * Set task running progress in current iteration
   * @param progress task running progress in current iteration
   */
  public void setProgress(float progress) {
    this.progress = progress;
  }

  /**
   * Get task running progress in current iteration
   * @return progress
   */
  public float getProgress() {
    return progress;
  }

  /**
   * Update calculate profiling counters
   * @param sampleNum calculate sample number
   * @param useTimeMs the time use to calculate the samples
   */
  public void updateProfileCounter(int sampleNum, int useTimeMs) {
    updateCounter(TaskCounter.TOTAL_CALCULATE_SAMPLES, sampleNum);
    updateCounter(TaskCounter.TOTAL_CALCULATE_TIME_MS, useTimeMs);
  }

  /**
   * Increment the counter
   * @param counterName counter name
   * @param updateValue increment value
   */
  public void updateCounter(String counterName, int updateValue) {
    AtomicLong counter = metrics.get(counterName);
    if(counter == null) {
      counter = metrics.putIfAbsent(counterName, new AtomicLong(0));
      if(counter == null) {
        counter = metrics.get(counterName);
      }
    }
    counter.addAndGet(updateValue);
  }

  /**
   * Update the counter
   * @param counterName counter name
   * @param updateValue new counter value
   */
  public void setCounter(String counterName, int updateValue) {
    AtomicLong counter = metrics.get(counterName);
    if(counter == null) {
      counter = metrics.putIfAbsent(counterName, new AtomicLong(0));
      if(counter == null) {
        counter = metrics.get(counterName);
      }
    }
    counter.set(updateValue);
  }

  public Map<String,AtomicLong> getCounters() {
    return metrics;
  }

  public Map<String,AtomicLong> getMetrics() {
    return metrics;
  }

  /**
   * Add a algorithm metric
   * @param name metric name
   * @param metric metric dependency values
   */
  public void addAlgoMetric(String name, Metric metric) {
    algoMetrics.put(name, metric);
  }
}
