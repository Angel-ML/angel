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

public enum AMPSAgentEventType {
  PSAGENT_SCHEDULE, PSAGENT_CONTAINER_LAUNCH_FAILED, PSAGENT_ATTEMPT_LAUNCHED, PSAGENT_ATTEMPT_FAILED, PSAGENT_ATTEMPT_KILLED, PSAGENT_DIAGNOSTICS_UPDATE, PSAGENT_UPDATE_STATE, PSAGENT_ERROR, PSAGENT_KILL, PSAGENT_ATTEMPT_SUCCESS, PSAGENT_ATTEMPT_REGISTERED,
}
