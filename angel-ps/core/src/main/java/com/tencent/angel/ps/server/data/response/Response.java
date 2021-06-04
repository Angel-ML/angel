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


package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.server.data.ServerState;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The result of the rpc from PSAgent to PS.
 */
public class Response implements Serialize {
  private static final Log LOG = LogFactory.getLog(Response.class);

  /**
   * Response header
   */
  private ResponseHeader header;

  /**
   * Response data
   */
  private ResponseData data;

  /**
   * Create a new Response.
   *
   * @param header response header(meta data)
   */
  public Response(ResponseHeader header, ResponseData data) {
    this.header = header;
    this.data = data;
  }

  /**
   * Create a new Response.
   */
  public Response() {
    this(null, null);
  }

  @Override public void serialize(ByteBuf buf) {
    header.serialize(buf);
    if(data != null) {
      ByteBufSerdeUtils.serializeBoolean(buf, true);
      data.serialize(buf);
    } else {
      ByteBufSerdeUtils.serializeBoolean(buf, false);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    header = new ResponseHeader();
    header.deserialize(buf);
    if(ByteBufSerdeUtils.deserializeBoolean(buf)) {
      data = ResponseFactory.createEmptyResponseData(TransportMethod.valueOf(header.methodId));
      data.deserialize(buf);
    }
  }

  @Override public int bufferLen() {
    int len = header.bufferLen();
    if(data != null) {
      len = data.bufferLen();
    }
    return len;
  }

  /**
   * Set response type.
   *
   * @param responseType response type
   */
  public void setResponseType(ResponseType responseType) {
    header.responseType = responseType;
  }

  /**
   * Get response type.
   *
   * @return responseType response type
   */
  public ResponseType getResponseType() {
    return header.responseType;
  }

  /**
   * Get detail error message.
   *
   * @return detail error message
   */
  public String getDetail() {
    return header.detail;
  }

  /**
   * Set detail error message.
   *
   * @param detail detail error message
   */
  public void setDetail(String detail) {
    header.detail = detail;
  }

  /**
   * Get server state
   *
   * @return server state
   */
  public ServerState getState() {
    return header.state;
  }

  /**
   * Set server state
   *
   * @param state server state
   */
  public void setState(ServerState state) {
    header.state = state;
  }

  @Override
  public String toString() {
    return "Response{" +
        "header=" + header +
        '}';
  }

  public ResponseHeader getHeader() {
    return header;
  }

  public void setHeader(ResponseHeader header) {
    this.header = header;
  }

  public ResponseData getData() {
    return data;
  }

  public void setData(ResponseData data) {
    this.data = data;
  }
}
