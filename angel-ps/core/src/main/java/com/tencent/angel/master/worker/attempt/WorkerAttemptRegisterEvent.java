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

package com.tencent.angel.master.worker.attempt;

import com.tencent.angel.common.location.Location;
import com.tencent.angel.worker.WorkerAttemptId;

/**
 * Worker attempt register to master.
 */
public class WorkerAttemptRegisterEvent extends WorkerAttemptEvent{
  /**the address of host on which the worker attempt is running on*/
  private final Location location;

  /**
   * Create a WorkerAttemptRegisterEvent
   * @param workerAttemptId worker attempt id
   * @param location worker attempt location
   */
  public WorkerAttemptRegisterEvent(WorkerAttemptId workerAttemptId, Location location) {
    super(WorkerAttemptEventType.REGISTER, workerAttemptId);
    this.location = location;
  }

  /**
   * Get worker attempt location
   * @return worker attempt location
   */
  public Location getLocation() {
    return location;
  }

}
