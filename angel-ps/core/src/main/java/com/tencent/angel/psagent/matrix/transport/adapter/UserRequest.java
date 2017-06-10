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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent.matrix.transport.adapter;

/**
 * The request of application layer.
 */
public abstract class UserRequest {
  /** request type */
  protected final UserRequestType type;

  /** clock value */
  protected final int clock;

  /**
   * Create a new UserRequest
   *
   * @param type request type
   * @param clock clock value
   */
  public UserRequest(UserRequestType type, int clock) {
    this.type = type;
    this.clock = clock;
  }

  /**
   * Get request type
   * 
   * @return UserRequestType request type
   */
  public UserRequestType getType() {
    return type;
  }

  /**
   * Get clock value
   * 
   * @return int clock value
   */
  public int getClock() {
    return clock;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + clock;
    result = prime * result + ((type == null) ? 0 : type.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    UserRequest other = (UserRequest) obj;
    if (clock != other.clock)
      return false;
    return type == other.type;
  }

  @Override
  public String toString() {
    return "UserRequest [type=" + type + ", clock=" + clock + "]";
  }
}
