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

import com.google.protobuf.ServiceException;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.exception.TimeOutException;
import com.tencent.angel.master.task.TaskCounter;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.client.MasterClient;
import com.tencent.angel.psagent.clock.ClockCache;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.WorkerContext;
import com.tencent.angel.worker.storage.DataBlockManager;
import com.tencent.angel.worker.storage.Reader;
import com.tencent.angel.protobuf.ProtobufUtil;
import com.tencent.angel.protobuf.generated.MLProtos.MatrixClock;
import com.tencent.angel.protobuf.generated.MLProtos.TaskIdProto;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.TaskMetaInfoProto;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The context for task of worker side.
 */
public class TaskContext {
  private final TaskId taskId;
  private final TaskIdProto taskIdProto;
  private final ConcurrentHashMap<String, AtomicLong> metrics;

  @SuppressWarnings("rawtypes")
  private Reader reader;
  private final com.tencent.angel.psagent.task.TaskContext context;

  /**
   * Instantiates context with task id.
   *
   * @param taskId the task id
   */
  public TaskContext(TaskId taskId) {
    this.taskId = taskId;
    this.taskIdProto = ProtobufUtil.convertToIdProto(taskId);
    this.metrics = new ConcurrentHashMap<>();
    context = PSAgentContext.get().getTaskContext(taskId.getIndex());
  }

  /**
   * Instantiates context with task meta.
   *
   * @param taskMeta the task meta
   */
  public TaskContext(TaskMetaInfoProto taskMeta) {
    taskIdProto = taskMeta.getTaskId();
    taskId = ProtobufUtil.convertToId(taskIdProto);
    context = PSAgentContext.get().getTaskContext(taskId.getIndex());
    context.setIteration(taskMeta.getIteration());
    this.metrics = new ConcurrentHashMap<>();
    List<MatrixClock> matrixClocks = taskMeta.getMatrixClockList();
    int size = matrixClocks.size();
    for(int i = 0; i < size; i++){
      context.setMatrixClock(matrixClocks.get(i).getMatrixId(), matrixClocks.get(i).getClock());
    }
  }

  /**
   * Gets reader.
   *
   * @param <K> key type
   * @param <V> value type
   * @return the reader
   * @throws ClassNotFoundException
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings("unchecked")
  public <K, V> Reader<K, V> getReader() throws ClassNotFoundException, IOException,
      InterruptedException {
    if (reader == null) {
      DataBlockManager dataBlockManager = WorkerContext.get().getDataBlockManager();
      reader = dataBlockManager.getReader(taskId);
    }
    return reader;
  }

  /**
   * Create matrix.
   *
   * @param matrixContext the matrix context
   * @param timeOutMs     the time out ms
   * @return the matrix meta
   * @throws ServiceException
   * @throws TimeOutException
   * @throws InterruptedException
   * @throws IOException
   */
  public MatrixMeta createMatrix(MatrixContext matrixContext, long timeOutMs)
      throws ServiceException, TimeOutException, InterruptedException, IOException {
    MasterClient masterClient = WorkerContext.get().getPSAgent().getMasterClient();
    MatrixMeta matrix = masterClient.createMatrix(matrixContext, timeOutMs);
    return matrix;
  }

  /**
   * Release matrix.
   *
   * @param matrix the matrix
   * @return the matrix meta
   * @throws ServiceException
   * @throws InterruptedException
   */
  public MatrixMeta releaseMatrix(MatrixMeta matrix) throws ServiceException, InterruptedException {
    MasterClient masterClient = WorkerContext.get().getPSAgent().getMasterClient();
    masterClient.releaseMatrix(matrix);
    return matrix;
  }

  /**
   * Gets task id.
   *
   * @return the task id
   */
  public TaskId getTaskId() {
    return taskId;
  }

  /**
   * Gets task's index.
   *
   * @return the task index
   */
  public int getTaskIndex() {
    return taskId.getIndex();
  }

  public TaskIdProto getTaskIdProto() {
    return taskIdProto;
  }

  /**
   * Gets context of psagent side.
   *
   * @return the context
   */
  public com.tencent.angel.psagent.task.TaskContext getContext() {
    return context;
  }

  /**
   * Get Task progress
   * @return Task progress
   */
  public float getProgress() {
    return context.getProgress();
  }

  /**
   * Set Task progress
   * @param progress  Task progress
   */
  public void setProgress(float progress) {
    context.setProgress(progress);
  }

  /**
   * Gets ps agent.
   *
   * @return the ps agent
   */
  public PSAgent getPSAgent() {
    return WorkerContext.get().getPSAgent();
  }

  /**
   * Gets matrix.
   *
   * @param matrixName the matrix name
   * @return the matrix
   * @throws Exception
   */
  public MatrixClient getMatrix(String matrixName) throws InvalidParameterException{
    return WorkerContext.get().getPSAgent().getMatrixClient(matrixName, taskId.getIndex());
  }

  /**
   * Gets conf.
   *
   * @return the conf
   */
  public Configuration getConf() {
    return WorkerContext.get().getConf();
  }

  /**
   * Gets total task num of current worker
   *
   * @return the total task num
   */
  public int getTotalTaskNum() {
    return WorkerContext.get().getActiveTaskNum();
  }

  /**
   * Global sync with special matrix,still wait until all matrixes's clock is synchronized.
   *
   * @param matrixId the matrix id
   * @throws InterruptedException
   */
  public void globalSync(int matrixId) throws InterruptedException {
    ClockCache clockCache = PSAgentContext.get().getClockCache();
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);

    int syncTimeIntervalMS =
      PSAgentContext
        .get()
        .getConf()
        .getInt(AngelConfiguration.ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS,
          AngelConfiguration.DEFAULT_ANGEL_PSAGENT_CACHE_SYNC_TIMEINTERVAL_MS);

    while (true) {
      boolean sync = true;
      for (PartitionKey pkey : pkeys) {
        if (clockCache.getClock(matrixId, pkey) < context.getMatrixClock(matrixId)) {
          sync = false;
          break;
        }
      }

      if (!sync) {
        Thread.sleep(syncTimeIntervalMS);
      } else {
        break;
      }
    }
  }

  /**
   * Global sync with all matrix.
   *
   * @throws InterruptedException
   */
  public void globalSync() throws InterruptedException {
    for (Integer matId: getMatrixClocks().keySet())
      globalSync(matId);
  }

  /**
   * Gets iteration num.
   *
   * @return the iteration
   */
  public int getIteration() {
    return context.getIteration();
  }

  /**
   * Increase iteration count.
   *
   * @throws ServiceException the service exception
   */
  public void incIteration() throws ServiceException{
    context.increaseIteration();
  }

  /**
   * Gets all matrix clocks.
   *
   * @return the clocks
   */
  public Map<Integer, AtomicInteger> getMatrixClocks() {
    return context.getMatrixClocks();
  }

  /**
   * Get matrix clock by matrix id
   *
   * @param matrixId the matrix id
   * @return the clock
   */
  public int getMatrixClock(int matrixId){
    return context.getMatrixClock(matrixId);
  }

  @Override
  public String toString() {
    return "TaskContext [taskId=" + taskId + ", taskIdProto=" + taskIdProto + ", context="
        + context + "]";
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
}
