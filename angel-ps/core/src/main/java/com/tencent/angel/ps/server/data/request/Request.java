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


package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class of rpc request from PSAgent to PS.
 */
public abstract class Request implements Serialize {
  /**
   * User requset id
   */
  private final int userRequestId;

  /**
   * Sub-request sequence id
   */
  private volatile int seqId;

  private final static AtomicInteger r = new AtomicInteger(0);
  private final int hashCode = r.incrementAndGet();

  /**
   * request context
   */
  private RequestContext context;

  /**
   * Create a new Request.
   *
   * @param context request context
   */
  public Request(RequestContext context) {
    this(-1, context);
  }

  /**
   * Create a new Request.
   *
   * @param userRequestId user request id
   * @param context       request context
   */
  public Request(int userRequestId, RequestContext context) {
    this.userRequestId = userRequestId;
    this.context = context;
  }

  /**
   * Create a new Request.
   */
  public Request() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {

  }

  @Override public void deserialize(ByteBuf buf) {

  }

  @Override public int bufferLen() {
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

  /**
   * Get user request id
   *
   * @return user request id
   */
  public int getUserRequestId() {
    return userRequestId;
  }

  /**
   * Get sub request sequence id
   *
   * @return sub request sequence id
   */
  public int getSeqId() {
    return seqId;
  }

  /**
   * Set sub request sequence id
   *
   * @param seqId sub request sequence id
   */
  public void setSeqId(int seqId) {
    this.seqId = seqId;
  }

  /**
   * Get request estimated serialize data size
   *
   * @return request estimated serialize data size
   */
  public abstract int getEstimizeDataSize();

  /**
   * Get request type
   *
   * @return request type
   */
  public abstract TransportMethod getType();

  @Override
  public boolean equals(Object o) {
    return false;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }
}
