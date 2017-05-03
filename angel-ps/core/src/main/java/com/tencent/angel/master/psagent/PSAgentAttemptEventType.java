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

public enum PSAgentAttemptEventType {
  PSAGENT_ATTEMPT_REGISTER, PSAGENT_ATTEMPT_CONTAINER_ASSIGNED, PSAGENT_ATTEMPT_CONTAINER_LAUNCHED, PSAGENT_ATTEMPT_CONTAINER_LAUNCH_FAILED, PSAGENT_ATTEMPT_SCHEDULE, PSAGENT_ATTEMPT_UPDATE_STATE, PSAGENT_ATTEMPT_UNREGISTER, PSAGENT_ATTEMPT_FAILMSG, PSAGENT_ATTEMPT_KILL, PSAGENT_ATTEMPT_COMMIT_FAILED, PSAGENT_ATTEMPT_COMMIT_SUCCESS, PSAGENT_ATTEMPT_SUCCESS, PSAGENT_ATTEMPT_CONTAINER_COMPLETE, PSAGENT_ATTEMPT_DIAGNOSTICS_UPDATE, PSAGENT_ATTEMPT_COMMIT
}
