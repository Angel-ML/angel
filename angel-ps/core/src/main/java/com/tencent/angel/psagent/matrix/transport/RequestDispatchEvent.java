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


package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.ps.server.data.request.Request;

/**
 * PS RPC request dispatch event.
 */
public class RequestDispatchEvent extends DispatcherEvent {
  /**
   * PS RPC request
   */
  private final Request request;

  /**
   * Create a new RequestDispatchEvent.
   *
   * @param type    event type
   * @param request rpc request
   */
  public RequestDispatchEvent(EventType type, Request request) {
    super(type);
    this.request = request;
  }

  /**
   * Get the request.
   *
   * @return Request the request
   */
  public Request getRequest() {
    return request;
  }
}
