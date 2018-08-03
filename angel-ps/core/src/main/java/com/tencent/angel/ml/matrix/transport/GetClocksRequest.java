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

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.ps.ParameterServerId;

/**
 * Get matrix partition clocks request.
 */
public class GetClocksRequest extends Request {
  private ParameterServerId serverId;

  /**
   * Create a new GetClocksRequest.
   *
   * @param serverId parameter server id
   */
  public GetClocksRequest(ParameterServerId serverId) {
    super(new RequestContext());
    this.serverId = serverId;
  }

  /**
   * Create a new GetClocksRequest.
   */
  public GetClocksRequest() {
    super();
  }

  @Override
  public int getEstimizeDataSize() {
    return 0;
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.GET_CLOCKS;
  }

  /**
   * Get the rpc dest ps
   * @return the rpc dest ps
   */
  public ParameterServerId getServerId() {
    return serverId;
  }
}
