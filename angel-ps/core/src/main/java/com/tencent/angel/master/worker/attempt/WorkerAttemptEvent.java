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

package com.tencent.angel.master.worker.attempt;

import com.tencent.angel.worker.WorkerAttemptId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Base class of worker attempt event.
 */
public class WorkerAttemptEvent extends AbstractEvent<WorkerAttemptEventType>{
  /**worker attempt id*/
  private final WorkerAttemptId workerAttemptId;

  /**
   * Create a WorkerAttemptEvent
   * @param type event type
   * @param workerAttemptId worker attempt id
   */
  public WorkerAttemptEvent(WorkerAttemptEventType type, WorkerAttemptId workerAttemptId) {
    super(type);
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
