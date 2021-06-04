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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class of rpc request from PSAgent to PS.
 */
public class Request implements Serialize {
  private final static AtomicInteger r = new AtomicInteger(0);
  private final transient int hashCode = r.incrementAndGet();

  /**
   * Request meta
   */
  private RequestHeader header;

  /**
   * Request data
   */
  private RequestData data;

  /**
   * request context
   */
  private transient RequestContext context;

  /**
   * serialized request data
   */
  private transient ByteBuf in;

  /**
   * Create a new Request.
   *
   * @param header Request header
   * @param context       request context
   */
  public Request(RequestHeader header, RequestData data, RequestContext context) {
    this.header = header;
    this.data = data;
    this.context = context;
  }

  public Request(RequestHeader header, RequestData data) {
    this(header, data, null);
  }

  public Request(RequestHeader header) {
    this(header, null, null);
  }

  /**
   * Create a new Request.
   */
  public Request(ByteBuf in) {
    this(null, null, null);
    this.in = in;
  }

  @Override public void serialize(ByteBuf buf) {
    serializeHeader(buf);
    serializeData(buf);
  }

  @Override public void deserialize(ByteBuf buf) {
    deserializeHeader(buf);
    deserializeData(buf);
  }

  public void serializeHeader(ByteBuf buf) {
    header.serialize(buf);
  }

  public void serializeData(ByteBuf buf) {
    if(data != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true);
      data.serialize(buf);
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false);
    }
  }

  public void deserializeHeader(ByteBuf buf) {
    header = new RequestHeader();
    header.deserialize(buf);
  }

  public void deserializeData(ByteBuf buf) {
    if(ByteBufSerdeUtils.deserializeBoolean(buf)) {
      data = RequestFactory.createEmptyRequestData(TransportMethod.valueOf(header.methodId));
      data.deserialize(buf);
    }
  }

  @Override public int bufferLen() {
    int len = header.bufferLen() + ByteBufSerdeUtils.BOOLEN_LENGTH;
    if(data != null) {
      len += data.bufferLen();
    }
    return len;
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
    return header.userRequestId;
  }

  /**
   * Get sub request sequence id
   *
   * @return sub request sequence id
   */
  public int getSeqId() {
    return header.seqId;
  }

  /**
   * Set sub request sequence id
   *
   * @param seqId sub request sequence id
   */
  public void setSeqId(int seqId) {
    header.seqId = seqId;
  }

  /**
   * Get how many elements handled by this rpc
   * @return need handle element number
   */
  public int getHandleElementNum() {
    return header.handleElemNum;
  }

  /**
   * Get token number
   *
   * @return token number
   */
  public int getTokenNum() {
    return header.token;
  }

  /**
   * Set token number
   *
   * @param tokenNum token number
   */
  public void setTokenNum(int tokenNum) {
    header.token = tokenNum;
  }

  /**
   * Get request type
   *
   * @return request type
   */
  public TransportMethod getType() {
    return TransportMethod.valueOf(header.methodId);
  }

  @Override
  public boolean equals(Object o) {
    return false;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  public boolean timeoutEnable() {
    return true;
  }

  @Override
  public String toString() {
    return "Request{" +
        "hashCode=" + hashCode +
        ", header=" + header +
        ", context=" + context +
        '}';
  }

  public RequestHeader getHeader() {
    return header;
  }

  public void setHeader(RequestHeader header) {
    this.header = header;
  }

  public RequestData getData() {
    return data;
  }

  public void setData(RequestData data) {
    this.data = data;
  }

  public void setContext(RequestContext context) {
    this.context = context;
  }
}
