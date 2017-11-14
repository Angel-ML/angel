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

import com.tencent.angel.ps.PSAttemptId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * Base class of ps attempt event.
 */
public class PSAttemptEvent extends AbstractEvent<PSAttemptEventType> {
  /**ps attempt id*/
  private final PSAttemptId psAttemptId;

  /**
   * Create a PSAttemptEvent
   * @param type event type
   * @param id ps attempt id
   */
  public PSAttemptEvent(PSAttemptEventType type, PSAttemptId id) {
    super(type);
    this.psAttemptId = id;
  }

  /**
   * Get ps attempt id
   * @return ps attempt id
   */
  public PSAttemptId getPSAttemptId() {
    return psAttemptId;
  }
}
