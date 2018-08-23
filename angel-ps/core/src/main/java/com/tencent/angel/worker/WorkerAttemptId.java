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
 * The class for worker attempt id
 */
public class WorkerAttemptId extends Id {

  protected static String WORKERATTEMPT = "WorkerAttempt";

  private WorkerId workerId;

  /**
   * Instantiates a new Worker attempt id.
   *
   * @param workerId     the worker id
   * @param attemptIndex the attempt index
   */
  public WorkerAttemptId(WorkerId workerId, int attemptIndex) {
    super(attemptIndex);
    this.workerId = workerId;
  }

  /**
   * Instantiates a new Worker attempt id.
   * <p> the 'idStr' must match <code>WorkerAttempt_XXX_XXX_XXX</code>
   *
   * @param idStr the id str
   * @throws UnvalidIdStrException
   */
  public WorkerAttemptId(String idStr) throws UnvalidIdStrException {
    if (idStr == null) {
      throw new UnvalidIdStrException("id str can not be null");
    }

    String[] idElemts = idStr.split(SEPARATOR);
    if (idElemts.length != 4 || !idElemts[0].equals(WORKERATTEMPT)) {
      throw new UnvalidIdStrException(
        "unvalid id str " + idStr + ", must be like this:" + WORKERATTEMPT + SEPARATOR
          + "workerGroupIndex" + SEPARATOR + "workerIndex" + SEPARATOR + "attemptIndex");
    }

    try {
      WorkerGroupId workerGroupId = new WorkerGroupId(Integer.valueOf(idElemts[1]));
      workerId = new WorkerId(workerGroupId, Integer.valueOf(idElemts[2]));
      index = Integer.valueOf(idElemts[3]);
    } catch (Exception x) {
      throw new UnvalidIdStrException(
        "unvalid id str " + idStr + " " + x.getMessage() + ", must be like this:" + WORKERATTEMPT
          + SEPARATOR + "workerGroupIndex" + SEPARATOR + "workerIndex" + SEPARATOR
          + "attemptIndex");
    }
  }

  /**
   * Gets worker id.
   *
   * @return the worker id
   */
  public WorkerId getWorkerId() {
    return workerId;
  }

  private StringBuilder appendTo(StringBuilder builder) {
    return workerId.appendTo(builder).append(SEPARATOR).append(index);
  }

  @Override public String toString() {
    return appendTo(new StringBuilder(WORKERATTEMPT)).toString();
  }

  @Override public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((workerId == null) ? 0 : workerId.hashCode());
    return result;
  }

  @Override public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    WorkerAttemptId other = (WorkerAttemptId) obj;
    if (workerId == null) {
      if (other.workerId != null)
        return false;
    } else if (!workerId.equals(other.workerId))
      return false;
    return true;
  }
}
