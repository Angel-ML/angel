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

package com.tencent.angel.ps;

import com.tencent.angel.common.Id;
import com.tencent.angel.exception.UnvalidIdStrException;

/**
 * The id represented parameter server.
 */
public class ParameterServerId extends Id {
  private static String PS = "ParameterServer";

  /**
   * Create a new Parameter server id.
   *
   * @param psIndex the ps index
   */
  public ParameterServerId(int psIndex) {
    super(psIndex);
  }

  /**
   * Create a new Parameter server id.
   *<p>
   *   'idStr' must match ParameterServer_XXX
   *</p>
   * @param idStr the id str
   * @throws UnvalidIdStrException
   */
  public ParameterServerId(String idStr) throws UnvalidIdStrException{
    if (idStr == null) {
      throw new UnvalidIdStrException("id str can not be null");
    }

    String[] idElemts = idStr.split(SEPARATOR);
    if (idElemts.length != 2 || !idElemts[0].equals(PS)) {
      throw new UnvalidIdStrException("unvalid id str " + idStr
          + ", must be like this:" + PS + SEPARATOR + "psIndex");
    }

    try {
      index = Integer.valueOf(idElemts[1]);
    } catch (Exception x) {
      throw new UnvalidIdStrException("unvalid id str " + idStr
          + ", must be like this:" + PS + SEPARATOR + "psIndex");
    }
  }

  /**
   * Append to string builder.
   *
   * @param builder the string builder
   * @return the string builder
   */
  public StringBuilder appendTo(StringBuilder builder) {
    return builder.append(SEPARATOR).append(index);
  }

  @Override
  public String toString() {
    return appendTo(new StringBuilder(PS)).toString();
  }
}
