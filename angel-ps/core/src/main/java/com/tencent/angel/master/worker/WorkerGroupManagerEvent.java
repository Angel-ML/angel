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

package com.tencent.angel.master.worker;

import com.tencent.angel.worker.WorkerGroupId;

/**
 * Worker group manager event.
 */
public class WorkerGroupManagerEvent extends WorkerManagerEvent{
  /**worker group id*/
  private final WorkerGroupId workerGroupId;

  /**
   * Create a WorkerGroupManagerEvent
   * @param type event type
   * @param workerGroupId worker group id
   */
  public WorkerGroupManagerEvent(WorkerManagerEventType type, WorkerGroupId workerGroupId) {
    super(type);
    this.workerGroupId = workerGroupId;
  }

  /**
   * Get worker group id
   * @return worker group id
   */
  public WorkerGroupId getWorkerGroupId() {
    return workerGroupId;
  }
}
