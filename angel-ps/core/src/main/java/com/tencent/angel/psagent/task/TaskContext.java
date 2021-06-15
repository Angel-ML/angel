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


package com.tencent.angel.psagent.task;

import com.tencent.angel.master.task.TaskCounter;
import com.tencent.angel.ml.metric.Metric;
import com.tencent.angel.psagent.matrix.storage.MatrixStorageManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Task running context. Task is executing unit for consistency control.
 */
public class TaskContext {
  private static final Log LOG = LogFactory.getLog(TaskContext.class);

  // task index, it must be unique for whole application
  private final int index;

  /**
   * Matrix storage for task
   */
  private final MatrixStorageManager matrixStorage;

  /**
   * Current epoch number
   */
  private final AtomicInteger epoch;

  /**
   * Task current epoch running progress
   */
  private volatile float progress;

  /**
   * Task system metrics
   */
  private final Map<String, AtomicLong> metrics;

  /**
   * Task algorithm metrics
   */
  private final Map<String, Metric> algoMetrics;

  /**
   * Create a TaskContext
   *
   * @param index task index
   */
  public TaskContext(int index) {
    this.index = index;
    this.matrixStorage = new MatrixStorageManager();
    this.epoch = new AtomicInteger(0);
    this.metrics = new ConcurrentHashMap<>();
    this.algoMetrics = new ConcurrentHashMap<>();
  }

  /**
   * Get task index
   *
   * @return task index
   */
  public int getIndex() {
    return index;
  }

  /**
   * Get the matrix storage manager for this task
   *
   * @return matrix storage manager
   */
  public MatrixStorageManager getMatrixStorage() {
    return matrixStorage;
  }

  /**
   * Get Task current epoch number
   *
   * @return current epoch number
   */
  public int getEpoch() {
    return epoch.get();
  }

  /**
   * Set the epoch number
   *
   * @param iterationValue epoch number
   */
  public void setEpoch(int iterationValue) {
    epoch.set(iterationValue);
  }

  /**
   * Set task running progress in current epoch
   *
   * @param progress task running progress in current epoch
   */
  public void setProgress(float progress) {
    this.progress = progress;
  }

  /**
   * Get task running progress in current epoch
   *
   * @return progress
   */
  public float getProgress() {
    return progress;
  }

  /**
   * Update calculate profiling counters
   *
   * @param sampleNum calculate sample number
   * @param useTimeMs the time use to calculate the samples
   */
  public void updateProfileCounter(int sampleNum, int useTimeMs) {
    updateCounter(TaskCounter.TOTAL_CALCULATE_SAMPLES, sampleNum);
    updateCounter(TaskCounter.TOTAL_CALCULATE_TIME_MS, useTimeMs);
  }

  /**
   * Increment the counter
   *
   * @param counterName counter name
   * @param updateValue increment value
   */
  public void updateCounter(String counterName, int updateValue) {
    AtomicLong counter = metrics.get(counterName);
    if (counter == null) {
      counter = metrics.putIfAbsent(counterName, new AtomicLong(0));
      if (counter == null) {
        counter = metrics.get(counterName);
      }
    }
    counter.addAndGet(updateValue);
  }

  /**
   * Update the counter
   *
   * @param counterName counter name
   * @param updateValue new counter value
   */
  public void setCounter(String counterName, int updateValue) {
    AtomicLong counter = metrics.get(counterName);
    if (counter == null) {
      counter = metrics.putIfAbsent(counterName, new AtomicLong(0));
      if (counter == null) {
        counter = metrics.get(counterName);
      }
    }
    counter.set(updateValue);
  }

  public Map<String, AtomicLong> getMetrics() {
    return metrics;
  }

  /**
   * Add a algorithm metric
   *
   * @param name   metric name
   * @param metric metric dependency values
   */
  public void addAlgoMetric(String name, Metric metric) {
    algoMetrics.put(name, metric);
  }

  public void increaseEpoch() {
    epoch.incrementAndGet();
  }
}
