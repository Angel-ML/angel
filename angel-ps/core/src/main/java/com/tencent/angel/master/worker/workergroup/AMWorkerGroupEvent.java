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
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Base class of worker group event.
 */
public class AMWorkerGroupEvent extends AbstractEvent<AMWorkerGroupEventType> {
  /**
   * worker group id
   */
  private final WorkerGroupId groupId;

  /**
   * Create a AMWorkerGroupEvent
   *
   * @param type    event type
   * @param groupId worker group id
   */
  public AMWorkerGroupEvent(AMWorkerGroupEventType type, WorkerGroupId groupId) {
    super(type);
    this.groupId = groupId;
  }

  /**
   * Get worker group id
   *
   * @return worker group id
   */
  public WorkerGroupId getGroupId() {
    return groupId;
  }

}
