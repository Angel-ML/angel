  
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

package com.tencent.angel.utils;

import com.tencent.angel.common.Id;
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.psagent.PSAgentAttemptId;
import com.tencent.angel.psagent.PSAgentId;

/**
 * Angel PS/Worker Id transform utils.
 */
public class IdUtils {

  public static PSAgentAttemptId convertToPSAgentAttemptId(String idStr) throws UnvalidIdStrException {
    if (idStr == null) {
      throw new UnvalidIdStrException("id str can not be null");
    }

    String[] idElemts = idStr.split(Id.SEPARATOR);
    if (idElemts.length != 3 || !idElemts[0].equals(PSAgentAttemptId.PSAGENTATTEMPT)) {
      throw new UnvalidIdStrException("unvalid id str " + idStr + ", must be like this:"
          + PSAgentAttemptId.PSAGENTATTEMPT + Id.SEPARATOR + "psAgnetIndex" + Id.SEPARATOR + "attemptIndex");
    }

    try {
      return new PSAgentAttemptId(new PSAgentId(Integer.valueOf(idElemts[1])), Integer.valueOf(idElemts[2]));
    } catch (Exception x) {
      throw new UnvalidIdStrException("unvalid id str " + idStr + ", must be like this:"
          + PSAgentAttemptId.PSAGENTATTEMPT + Id.SEPARATOR + "psAgnetIndex" + Id.SEPARATOR + "attemptIndex");
    }
  }
  
  public static PSAgentId convertToPSAgentId(String idStr) throws UnvalidIdStrException{
    if (idStr == null) {
      throw new UnvalidIdStrException("id str can not be null");
    }

    String[] idElemts = idStr.split(Id.SEPARATOR);
    if (idElemts.length != 2 || !idElemts[0].equals(PSAgentId.PSAGENT)) {
      throw new UnvalidIdStrException("unvalid id str " + idStr
          + ", must be like this:" + PSAgentId.PSAGENT + Id.SEPARATOR + "index");
    }

    try {
      return new PSAgentId(Integer.valueOf(idElemts[1]));
    } catch (Exception x) {
      throw new UnvalidIdStrException("unvalid id str " + idStr
          + ", must be like this:" + PSAgentId.PSAGENT + Id.SEPARATOR + "index");
    }
  }
}
