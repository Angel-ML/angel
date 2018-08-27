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


package com.tencent.angel.master.worker.workergroup;

import com.tencent.angel.worker.WorkerGroupId;
import com.tencent.angel.worker.WorkerId;

/**
 * Worker group events come from the workers in worker group.
 */
public class WorkerGroupFromWorkerEvent extends AMWorkerGroupEvent {
  /**
   * worker id
   */
  private final WorkerId workerId;

  /**
   * Create a WorkerGroupFromWorkerEvent
   *
   * @param type     event type
   * @param groupId  worker group id
   * @param workerId worker id
   */
  public WorkerGroupFromWorkerEvent(AMWorkerGroupEventType type, WorkerGroupId groupId,
    WorkerId workerId) {
    super(type, groupId);
    this.workerId = workerId;
  }

  /**
   * Get worker id
   *
   * @return worker id
   */
  public WorkerId getWorkerId() {
    return workerId;
  }

}
