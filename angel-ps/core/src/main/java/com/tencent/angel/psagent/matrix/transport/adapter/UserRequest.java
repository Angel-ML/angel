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


package com.tencent.angel.psagent.matrix.transport.adapter;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * The request of application layer.
 */
public abstract class UserRequest {
  /**
   * Request id
   */
  private final int requestId;

  /**
   * request type
   */
  protected final UserRequestType type;

  /**
   * Request id generator
   */
  private static final AtomicInteger idGen = new AtomicInteger(0);

  /**
   * Create a new UserRequest
   *
   * @param type request type
   */
  public UserRequest(UserRequestType type) {
    this.requestId = idGen.incrementAndGet();
    this.type = type;
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
   * Get request id
   *
   * @return request id
   */
  public int getRequestId() {
    return requestId;
  }

  @Override public String toString() {
    return "UserRequest{" + "requestId=" + requestId + ", type=" + type + '}';
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    UserRequest that = (UserRequest) o;

    if (requestId != that.requestId)
      return false;
    return type == that.type;
  }

  @Override public int hashCode() {
    int result = requestId;
    result = 31 * result + (type != null ? type.hashCode() : 0);
    return result;
  }
}