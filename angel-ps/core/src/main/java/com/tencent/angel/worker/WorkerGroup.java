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

import com.tencent.angel.split.SplitClassification;

import java.util.HashMap;
import java.util.Map;

/**
 * The group of worker
 */
public class WorkerGroup {
  private final WorkerGroupId workerGroupId;
  private final Map<WorkerId, WorkerRef> workerMap;
  private final SplitClassification splits;

  /**
   * Instantiates a new worker group.
   *
   * @param workerGroupId the worker group id
   * @param splits        the splits
   */
  public WorkerGroup(WorkerGroupId workerGroupId, SplitClassification splits) {
    this.workerGroupId = workerGroupId;
    this.splits = splits;
    workerMap = new HashMap<WorkerId, WorkerRef>();
  }

  /**
   * Gets worker group id.
   *
   * @return the worker group id
   */
  public WorkerGroupId getWorkerGroupId() {
    return workerGroupId;
  }

  /**
   * Gets worker map.
   *
   * @return the worker map
   */
  public Map<WorkerId, WorkerRef> getWorkerMap() {
    return workerMap;
  }

  /**
   * Add worker ref.
   *
   * @param workerRef the worker ref
   */
  public void addWorkerRef(WorkerRef workerRef) {
    workerMap.put(workerRef.getWorkerAttemptId().getWorkerId(), workerRef);
  }

  /**
   * Remove worker ref.
   *
   * @param workerRef the worker ref
   */
  public void removeWorkerRef(WorkerRef workerRef) {
    workerMap.remove(workerRef.getWorkerAttemptId().getWorkerId());
  }

  /**
   * Remove worker ref.
   *
   * @param id the id
   */
  public void removeWorkerRef(WorkerId id) {
    workerMap.remove(id);
  }

  /**
   * Gets worker ref.
   *
   * @param id the id
   * @return the worker ref
   */
  public WorkerRef getWorkerRef(WorkerId id) {
    return workerMap.get(id);
  }

  /**
   * Gets splits.
   *
   * @return the splits
   */
  public SplitClassification getSplits() {
    return splits;
  }
}