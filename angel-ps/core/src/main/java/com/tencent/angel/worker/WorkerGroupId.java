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
 * The type worker group id.
 */
public class WorkerGroupId extends Id {
  public static String WORKERGROUP = "WorkerGroup";

  /**
   * Instantiates a new Worker group id with index.
   *
   * @param index the index
   */
  public WorkerGroupId(int index) {
    super(index);
  }

  /**
   * Instantiates a new Worker group id with 'idStr'
   * <p>
   * 'idStr' must match <code>WorkerGroup_XXX</code>
   *
   * @param idStr the id str
   * @throws UnvalidIdStrException
   */
  public WorkerGroupId(String idStr) throws UnvalidIdStrException {
    if (idStr == null) {
      throw new UnvalidIdStrException("workergroup id str can not be null");
    }

    String[] idElemts = idStr.split(SEPARATOR);
    if (idElemts.length != 2 || !idElemts[0].equals(WORKERGROUP)) {
      throw new UnvalidIdStrException(
        "unvalid id str " + idStr + ", must be like this:" + WORKERGROUP + SEPARATOR
          + "workerGroupIndex");
    }

    try {
      index = Integer.valueOf(idElemts[1]);
    } catch (Exception x) {
      throw new UnvalidIdStrException(
        "unvalid id str " + idStr + ", must be like this:" + WORKERGROUP + SEPARATOR
          + "workerGroupIndex");
    }
  }

  @Override public String toString() {
    return WORKERGROUP + Id.SEPARATOR + super.toString();
  }
}
