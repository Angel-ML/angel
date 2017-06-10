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
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.index.ColumnIndex;
import com.tencent.angel.psagent.matrix.index.MatrixIndex;
import com.tencent.angel.psagent.matrix.storage.MatrixStorageManager;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Task running context. Task is executing unit for consistency control.
 */
public class TaskContext {

  // matrix id to clock map
  private final ConcurrentHashMap<Integer, AtomicInteger> matrixIdToClockMap;

  // task index, it must be unique for whole application
  private final int index;

  // matrix id to task update index map. each task may only update some specific part for a matrix
  private final ConcurrentHashMap<Integer, MatrixIndex> matrixIndexes;

  private final MatrixStorageManager matrixStorage;
  
  private final AtomicInteger iteration;

  private final boolean syncClockEnable;

  private volatile float progress;

  public TaskContext(int index) {
    this.index = index;
    this.matrixIndexes = new ConcurrentHashMap<Integer, MatrixIndex>();
    this.matrixIdToClockMap = new ConcurrentHashMap<Integer, AtomicInteger>();
    this.matrixStorage = new MatrixStorageManager();
    this.iteration = new AtomicInteger(0);
    
    syncClockEnable =
        PSAgentContext
            .get()
            .getConf()
            .getBoolean(AngelConfiguration.ANGEL_PSAGENT_SYNC_CLOCK_ENABLE,
                AngelConfiguration.DEFAULT_ANGEL_PSAGENT_SYNC_CLOCK_ENABLE);
  }

  public int getIndex() {
    return index;
  }

  public int getMatrixClock(int matrixId) {
    if (!matrixIdToClockMap.containsKey(matrixId)) {
      matrixIdToClockMap.putIfAbsent(matrixId, new AtomicInteger(0));
    }
    return matrixIdToClockMap.get(matrixId).get();
  }

  @SuppressWarnings("unused")
  private ColumnIndex getColumnIndex(int matrixId, int rowIndex) {
    MatrixIndex matrixIndex = getMatrixIndex(matrixId);
    if (matrixIndex == null) {
      return null;
    }
    return matrixIndex.getColumnIndex(rowIndex);
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

  public void increaseMatrixClock(int matrixId) {
    if (!matrixIdToClockMap.containsKey(matrixId)) {
      matrixIdToClockMap.putIfAbsent(matrixId, new AtomicInteger(0));
    }
    matrixIdToClockMap.get(matrixId).incrementAndGet();
  }

  public MatrixStorageManager getMatrixStorage() {
    return matrixStorage;
  }

  public int getIteration() {
    return iteration.get();
  }
  
  public void increaseIteration() throws ServiceException {
    int iterationValue = iteration.incrementAndGet();
    if(syncClockEnable){
      PSAgentContext.get().getMasterClient().taskIteration(index, iterationValue);
    }
  }
  
  public void setIteration(int iterationValue){
    iteration.set(iterationValue);
  }
  
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

  public Map<Integer, AtomicInteger> getMatrixClocks() {
    return matrixIdToClockMap;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public float getProgress() {
    return progress;
  }
}
