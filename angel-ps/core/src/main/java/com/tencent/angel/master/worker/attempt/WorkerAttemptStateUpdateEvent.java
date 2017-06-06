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

import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.WorkerReportRequest;
import com.tencent.angel.worker.WorkerAttemptId;

/**
 * Worker attempt report its state to master.
 */
public class WorkerAttemptStateUpdateEvent extends WorkerAttemptEvent{
  /**worker attempt report*/
  private final WorkerReportRequest report;

  /**
   * Create a WorkerAttemptStateUpdateEvent
   * @param workerAttemptId worker attempt id
   * @param report state update report
   */
  public WorkerAttemptStateUpdateEvent(WorkerAttemptId workerAttemptId, WorkerReportRequest report) {
    super(WorkerAttemptEventType.UPDATE_STATE, workerAttemptId);
    this.report = report;
  }

  /**
   * Get worker attempt report
   * @return worker attempt report
   */
  public WorkerReportRequest getReport() {
    return report;
  }

}
