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

package com.tencent.angel.master.ps.attempt;

import org.apache.hadoop.yarn.api.records.Container;

import com.tencent.angel.ps.PSAttemptId;

/**
 * Assign a container for a ps.
 */
public class PSAttemptContainerAssignedEvent extends PSAttemptEvent {
  /**the container allocated for the ps attempt*/
  private final Container container;

  /**
   * Create a PSAttemptContainerAssignedEvent
   * @param id ps attempt id
   * @param container container
   */
  public PSAttemptContainerAssignedEvent(PSAttemptId id, Container container) {
    super(PSAttemptEventType.PA_CONTAINER_ASSIGNED, id);
    this.container = container;
  }

  /**
   * Get container
   * @return container
   */
  public Container getContainer() {
    return container;
  }
}
