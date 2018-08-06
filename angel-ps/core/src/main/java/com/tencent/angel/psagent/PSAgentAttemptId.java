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

package com.tencent.angel.psagent;

import com.tencent.angel.common.Id;

/**
 * PS Agent attempt id
 */
public class PSAgentAttemptId extends Id {
  public static String PSAGENTATTEMPT = "PSAgentAttempt";
  private PSAgentId psAgentId;

  public PSAgentAttemptId(PSAgentId psAgentId, int attemptIndex) {
    super(attemptIndex);
    this.psAgentId = psAgentId;
  }

  protected StringBuilder appendTo(StringBuilder builder) {
    return psAgentId.appendTo(builder).append(SEPARATOR).append(index);
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(PSAGENTATTEMPT)).toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((psAgentId == null) ? 0 : psAgentId.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!super.equals(obj))
      return false;
    if (getClass() != obj.getClass())
      return false;
    PSAgentAttemptId other = (PSAgentAttemptId) obj;
    if (psAgentId == null) {
      if (other.psAgentId != null)
        return false;
    } else if (!psAgentId.equals(other.psAgentId))
      return false;
    return true;
  }

  public PSAgentId getPsAgentId() {
    return psAgentId;
  }
}
