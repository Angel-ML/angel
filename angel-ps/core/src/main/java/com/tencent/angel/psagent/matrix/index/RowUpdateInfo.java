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

package com.tencent.angel.psagent.matrix.index;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * This class store the update clock info of every row and task at matrix oplog
 */
public class RowUpdateInfo {

  private static final Log LOG = LogFactory.getLog(RowUpdateInfo.class);

  // array stores clock for each row and clock
  private short[] info;
  private int rowNum;
  // local task num
  private int taskNum;

  // mapping from task index to taskId
  private short[] taskIndexToId;
  // mapping from taskId to task index
  private Int2IntOpenHashMap taskIdToIndex;

  public RowUpdateInfo(int rowNum) {
    this.taskNum = -1;
    this.rowNum = rowNum;
  }

  public void init() {
    this.taskNum = 0;// MLContext.get().getLocalTaskNum();
    this.info = new short[this.rowNum * taskNum];
    this.taskIndexToId = new short[taskNum];
    this.taskIdToIndex = new Int2IntOpenHashMap();

    LOG.info("local task num=" + taskNum);

    int[] taskIds = new int[0];
    Arrays.sort(taskIds);

    for (int i = 0; i < taskIds.length; i++) {
      taskIndexToId[i] = (short) taskIds[i];
      taskIdToIndex.put(taskIds[i], i);
    }
  }

  public void startIfNeed() {
    if (taskNum == -1) {
      init();
    }
  }

  public int getRowClockWithTaskId(int rowId, int taskId) {
    int taskIndex = getTaskIndex(taskId);
    return getRowClockWithIndex(rowId, taskIndex);
  }

  public int getRowClockWithIndex(int rowId, int taskIndex) {
    int arrayIndex = getArrayIndex(rowId, taskIndex);
    return info[arrayIndex];
  }

  public int getTaskId(int taskIndex) {
    return taskIndexToId[taskIndex];
  }

  public int getTaskIndex(int taskId) {
    return taskIdToIndex.get(taskId);
  }

  public void setRowClockWithTaskId(int rowId, int clock, int taskId) {
    startIfNeed();
    setRowClockWithIndex(rowId, clock, getTaskIndex(taskId));
  }

  public void setRowClockWithIndex(int rowId, int clock, int taskIndex) {
    startIfNeed();
    int arrayIndex = getArrayIndex(rowId, taskIndex);
    info[arrayIndex] = (short) clock;
  }

  private int getArrayIndex(int rowId, int taskIndex) {
    return rowId / taskNum + taskIndex;
  }

  public int getTaskNum() {
    return taskNum;
  }
}
