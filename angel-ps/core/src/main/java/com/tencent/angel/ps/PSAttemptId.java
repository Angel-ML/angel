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

package com.tencent.angel.ps;

import com.tencent.angel.common.Id;
import com.tencent.angel.exception.UnvalidIdStrException;

/**
 * The id represents parameter server attempt.
 */
public class PSAttemptId extends Id {

  private static String PSATTEMPT = "PSAttempt";
  private ParameterServerId psId;

  /**
   * Create a new Ps attempt id.
   *
   * @param psId         the ps id
   * @param attemptIndex the attempt index
   */
  public PSAttemptId(ParameterServerId psId, int attemptIndex) {
    super(attemptIndex);
    this.psId = psId;
  }

  /**
   * Create a new Ps attempt id.
   *<p>
   *   'idStr' must match PSAttempt_XXX_XXX
   *</p>
   * @param idStr the id str
   * @throws UnvalidIdStrException the unvalid id str exception
   */
  public PSAttemptId(String idStr) throws UnvalidIdStrException{
    if (idStr == null) {
      throw new UnvalidIdStrException("id str can not be null");
    }

    String[] idElemts = idStr.split(SEPARATOR);
    if (idElemts.length != 3 || !idElemts[0].equals(PSATTEMPT)) {
      throw new UnvalidIdStrException("unvalid id str " + idStr + ", must be like this:" + PSATTEMPT
          + SEPARATOR + "psIndex" + SEPARATOR + "attemptIndex");
    }

    try {
      psId = new ParameterServerId(Integer.valueOf(idElemts[1]));
      index = Integer.valueOf(idElemts[2]);
    } catch (Exception x) {
      throw new UnvalidIdStrException("unvalid id str " + idStr + ", must be like this:" + PSATTEMPT
          + SEPARATOR + "psIndex" + SEPARATOR + "attemptIndex");
    }
  }

  /**
   * Gets parameter server id.
   *
   * @return the parameter server id
   */
  public ParameterServerId getPsId() {
    return psId;
  }

  /**
   * Append to string builder.
   *
   * @param builder the string builder
   * @return the string builder
   */
  protected StringBuilder appendTo(StringBuilder builder) {
    return psId.appendTo(builder).append(SEPARATOR).append(index);
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(PSATTEMPT)).toString();
  }
  
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((psId == null) ? 0 : psId.hashCode());
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
    PSAttemptId other = (PSAttemptId) obj;
    if (psId == null) {
      if (other.psId != null)
        return false;
    } else if (!psId.equals(other.psId))
      return false;
    return true;
  }
}
