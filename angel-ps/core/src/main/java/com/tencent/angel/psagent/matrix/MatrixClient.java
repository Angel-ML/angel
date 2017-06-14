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

package com.tencent.angel.psagent.matrix;

import java.util.concurrent.ExecutionException;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.enhance.ZeroUpdate;
import com.tencent.angel.psagent.task.TaskContext;

/**
 * The base class of matrix client used by ps client. It contains a task context which use to SSP
 * consistency control.
 */
public abstract class MatrixClient implements MatrixInterface {
  /** matrix id */
  protected int matrixId;

  /** task context */
  protected TaskContext taskContext;

  /**
   * Set matrix id.
   * 
   * @param matrixId matrix id
   */
  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
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
   * Set task context.
   * 
   * @param taskContext task context
   */
  public void setTaskContext(TaskContext taskContext) {
    this.taskContext = taskContext;
  }

  /**
   * Get task context.
   * 
   * @return task context
   */
  public TaskContext getTaskContext() {
    return taskContext;
  }

  public void zero() throws AngelException {
    ZeroUpdate updater = new ZeroUpdate(new ZeroUpdate.ZeroUpdateParam(getMatrixId(), false));
    try{
      update(updater).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new AngelException(e);
    }
  }
}
