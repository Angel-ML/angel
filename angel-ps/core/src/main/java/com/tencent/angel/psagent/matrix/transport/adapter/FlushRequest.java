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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLog;

/**
 * Flush matrix oplog to ps request.
 */
public class FlushRequest extends UserRequest {
  /** task index */
  private final int taskIndex;

  /** need update clock or not */
  private final boolean updateClock;

  /** matrix id */
  private final int matrixId;

  /** matrix oplog */
  private final MatrixOpLog opLog;

  /**
   * 
   * Create a new FlushRequest.
   *
   * @param clock matrix clock value
   * @param taskIndex task index
   * @param matrixId matrix id
   * @param matrixOpLog matrix oplog
   * @param updateClock true means we need update clock after update the matrix, false means we just
   *        update the matrix
   */
  public FlushRequest(int clock, int taskIndex, int matrixId, MatrixOpLog matrixOpLog,
      boolean updateClock) {
    super(UserRequestType.FLUSH, clock);
    this.taskIndex = taskIndex;
    this.matrixId = matrixId;
    this.opLog = matrixOpLog;
    this.updateClock = updateClock;
  }

  /**
   * Get task index.
   * 
   * @return int task index
   */
  public int getTaskIndex() {
    return taskIndex;
  }

  /**
   * If we need update the matrix clock.
   * 
   * @return boolean true means we need update clock after update the matrix, false means we just
   *         update the matrix
   */
  public boolean isUpdateClock() {
    return updateClock;
  }

  /**
   * Get matrix id.
   * 
   * @return int matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  /**
   * Get matrix oplog
   * 
   * @return MatrixOpLog matrix oplog
   */
  public MatrixOpLog getOpLog() {
    return opLog;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + matrixId;
    result = prime * result + ((opLog == null) ? 0 : opLog.hashCode());
    result = prime * result + taskIndex;
    result = prime * result + (updateClock ? 1231 : 1237);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    return (this == obj);
  }
}
