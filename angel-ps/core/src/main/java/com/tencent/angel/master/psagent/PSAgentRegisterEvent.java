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

package com.tencent.angel.master.psagent;

import com.tencent.angel.common.Location;
import com.tencent.angel.psagent.PSAgentAttemptId;

public class PSAgentRegisterEvent extends PSAgentManagerEvent {
  private final PSAgentAttemptId attemptId;
  private final Location location;

  public PSAgentRegisterEvent(PSAgentAttemptId attemptId, Location location) {
    super(PSAgentManagerEventType.PSAGENT_REGISTER, attemptId.getPsAgentId());
    this.location = location;
    this.attemptId = attemptId;
  }

  public Location getLocation() {
    return location;
  }

  public PSAgentAttemptId getAttemptId() {
    return attemptId;
  }

}
