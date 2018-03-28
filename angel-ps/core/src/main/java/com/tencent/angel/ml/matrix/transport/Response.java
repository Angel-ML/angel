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
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.UnsupportedEncodingException;

/**
 * The result of the rpc from PSAgent to PS.
 */
public class Response implements Serialize {
  private static final Log LOG = LogFactory.getLog(Response.class);

  /** Server state:IDLE, GENERAL, BUSY*/
  private ServerState state;

  /** response type */
  private ResponseType responseType;

  /** detail error message */
  private String detail;

  //public String uuid;

  /**
   * Create a new Response.
   *
   * @param responseType response type
   * @param detail detail error message
   */
  public Response(ResponseType responseType, String detail) {
    this.responseType = responseType;
    this.detail = detail;
  }

  /**
   * Create a new Response.
   * 
   * @param responseType response type
   */
  public Response(ResponseType responseType) {
    this(responseType, null);
  }

  /**
   * Create a new Response.
   */
  public Response() {
    this(ResponseType.SUCCESS, null);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(responseType.getTypeId());
    buf.writeInt(state.getTypeId());
    try {
      if (detail != null && !detail.isEmpty()) {
        byte[] serializedErrMsg = detail.getBytes("utf-8");
        buf.writeInt(serializedErrMsg.length);
        buf.writeBytes(detail.getBytes("utf-8"));
      } else {
        buf.writeInt(0);
      }
    } catch (UnsupportedEncodingException e) {
      LOG.error("serialize error message failed, ", e);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    responseType = ResponseType.valueOf(buf.readInt());
    state = ServerState.valueOf(buf.readInt());
    int len = buf.readInt();
    if (len != 0) {
      byte[] detailData = new byte[len];
      buf.readBytes(detailData);
      try {
        detail = new String(detailData, "utf-8");
      } catch (UnsupportedEncodingException e) {
        LOG.error("deserialize error message failed, ", e);;
      }
    }
  }

  @Override
  public int bufferLen() {
    return 4;
  }

  /**
   * Set response type.
   * 
   * @param responseType response type
   */
  public void setResponseType(ResponseType responseType) {
    this.responseType = responseType;
  }

  /**
   * Get response type.
   * 
   * @return responseType response type
   */
  public ResponseType getResponseType() {
    return responseType;
  }

  /**
   * Get detail error message.
   * 
   * @return detail error message
   */
  public String getDetail() {
    return detail;
  }

  /**
   * Set detail error message.
   * 
   * @param detail detail error message
   */
  public void setDetail(String detail) {
    this.detail = detail;
  }

  /**
   * Clear RPC Get result
   */
  public void clear() {}

  /**
   * Get server state
   * @return server state
   */
  public ServerState getState() {
    return state;
  }

  /**
   * Set server state
   * @param state server state
   */
  public void setState(ServerState state) {
    this.state = state;
  }

  @Override public String toString() {
    return "Response{" + "state=" + state + ", responseType=" + responseType + ", detail='" + detail
      + '\'' + '}';
  }
}
