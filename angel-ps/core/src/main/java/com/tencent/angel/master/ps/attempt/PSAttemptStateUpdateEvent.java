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

package com.tencent.angel.master.ps.attempt;

import com.tencent.angel.ps.PSAttemptId;

import java.util.Map;

/**
 * Update the counters for ps attempt.
 */
public class PSAttemptStateUpdateEvent extends PSAttemptEvent {
  /**ps attempt counters*/
  private final Map<String, String> params;

  /**
   * Create a PSAttemptStateUpdateEvent
   * @param id ps attempt id
   * @param params counters
   */
  public PSAttemptStateUpdateEvent(PSAttemptId id, Map<String, String> params) {
    super(PSAttemptEventType.PA_UPDATE_STATE, id);
    this.params = params;
  }

  /**
   * Get counters
   * @return counters
   */
  public Map<String, String> getParams() {
    return params;
  }
}
