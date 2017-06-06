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

package com.tencent.angel.master.worker.worker;

import com.tencent.angel.worker.WorkerAttemptId;

/**
 * Worker events that come from the attempts of worker.
 */
public class WorkerFromAttemptEvent extends AMWorkerEvent{
  /**worker attempt id*/
  private final WorkerAttemptId workerAttemptId;

  /**
   * Create a WorkerFromAttemptEvent
   * @param eventType event type
   * @param workerAttemptId worker attempt id
   */
  public WorkerFromAttemptEvent(AMWorkerEventType eventType, WorkerAttemptId workerAttemptId) {
    super(eventType, workerAttemptId.getWorkerId());
    this.workerAttemptId = workerAttemptId;
  }

  /**
   * Get worker attempt id
   * @return worker attempt id
   */
  public WorkerAttemptId getWorkerAttemptId() {
    return workerAttemptId;
  }

}
