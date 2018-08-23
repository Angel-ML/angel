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


package com.tencent.angel.worker;

import com.tencent.angel.common.Id;
import com.tencent.angel.exception.UnvalidIdStrException;

/**
 * The type Worker id.
 */
public class WorkerId extends Id {
  protected static String WORKER = "Worker";
  private WorkerGroupId workerGroupId;

  /**
   * Instantiates a new worker id with 'workerGroupId' and 'workerIndex'
   *
   * @param workerGroupId the worker group id
   * @param workerIndex   the worker index
   */
  public WorkerId(WorkerGroupId workerGroupId, int workerIndex) {
    super(workerIndex);
    this.workerGroupId = workerGroupId;
  }

  /**
   * Instantiates a new Worker id with 'idStr'
   * <p>
   * 'idStr' must match <code>Worker_XXX_XXX</code>
   * </p>
   *
   * @param idStr the id str
   * @throws UnvalidIdStrException the unvalid id str exception
   */
  public WorkerId(String idStr) throws UnvalidIdStrException {
    if (idStr == null) {
      throw new UnvalidIdStrException("id str can not be null");
    }

    String[] idElemts = idStr.split(SEPARATOR);
    if (idElemts.length != 3 || !idElemts[0].equals(WORKER)) {
      throw new UnvalidIdStrException(
        "unvalid id str " + idStr + ", must be like this:" + WORKER + SEPARATOR + "workerGroupIndex"
          + SEPARATOR + "workerIndex");
    }

    try {
      workerGroupId = new WorkerGroupId(Integer.valueOf(idElemts[1]));
      index = Integer.valueOf(idElemts[2]);
    } catch (Exception x) {
      throw new UnvalidIdStrException(
        "unvalid id str " + idStr + " " + x.getMessage() + ", must be like this:" + WORKER
          + SEPARATOR + "workerGroupIndex" + SEPARATOR + "workerIndex");
    }
  }

  /**
   * Gets worker group id.
   *
   * @return the worker group id
   */
  public WorkerGroupId getWorkerGroupId() {
    return workerGroupId;
  }

  protected StringBuilder appendTo(StringBuilder builder) {
    return builder.append(SEPARATOR).append(workerGroupId.getIndex()).append(SEPARATOR)
      .append(index);
  }

  @Override public String toString() {
    return appendTo(new StringBuilder(WORKER)).toString();
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((workerGroupId == null) ? 0 : workerGroupId.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    WorkerId other = (WorkerId) obj;
    if (workerGroupId == null) {
      if (other.workerGroupId != null)
        return false;
    } else if (!workerGroupId.equals(other.workerGroupId))
      return false;
    return true;
  }
}
