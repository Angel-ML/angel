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

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;

/**
 * Base class of rpc request from PSAgent to PS.
 */
public abstract class Request implements Serialize {

  /** parameter server id */
  private ParameterServerId serverId;

  /** request context */
  private RequestContext context;

  /**
   * Create a new Request.
   *
   * @param serverId parameter server id
   * @param context request context
   */
  private Request(ParameterServerId serverId, RequestContext context) {
    this.serverId = serverId;
    this.context = context;
  }

  /**
   * Create a new Request.
   *
   * @param serverId parameter server id
   */
  public Request(ParameterServerId serverId) {
    this(serverId, new RequestContext());
  }

  /**
   * Create a new Request.
   */
  public Request() {
    this(null, null);
  }

  @Override
  public void serialize(ByteBuf buf) {

  }

  @Override
  public void deserialize(ByteBuf buf) {

  }

  @Override
  public int bufferLen() {
    return 0;
  }

  /**
   * Get rpc request context.
   * 
   * @return RequestContext rpc request context
   */
  public RequestContext getContext() {
    return context;
  }

  public abstract int getEstimizeDataSize();

  /**
   * Get parameter server id.
   * 
   * @return ParameterServerId parameter server id.
   */
  public ParameterServerId getServerId() {
    return serverId;
  }

  /**
   * Set parameter server id.
   * 
   * @param serverId parameter server id
   */
  public void setServerId(ParameterServerId serverId) {
    this.serverId = serverId;
  }

  public abstract TransportMethod getType();

  @Override public String toString() {
    return "Request{" + "serverId=" + serverId + '}';
  }
}
