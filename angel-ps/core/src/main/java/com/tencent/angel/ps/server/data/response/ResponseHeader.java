package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.server.data.ServerState;
import io.netty.buffer.ByteBuf;

public class ResponseHeader implements Serialize {
  public int seqId;
  public int methodId;
  public ServerState state;
  public ResponseType responseType;
  public String detail;

  public ResponseHeader(int seqId, int methodId, ServerState state, ResponseType responseType, String detail) {
    this.seqId = seqId;
    this.methodId = methodId;
    this.state = state;
    this.responseType = responseType;
    this.detail = detail;
  }

  public ResponseHeader(int seqId, int methodId, ServerState state, ResponseType responseType) {
    this(seqId, methodId, state, responseType, "");
  }

  public ResponseHeader() {
    this(-1, -1, ServerState.GENERAL, ResponseType.SUCCESS, "");
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, seqId);
    ByteBufSerdeUtils.serializeInt(output, methodId);
    ByteBufSerdeUtils.serializeInt(output, state.getTypeId());
    ByteBufSerdeUtils.serializeInt(output, responseType.getTypeId());
    ByteBufSerdeUtils.serializeUTF8(output, detail);
  }

  @Override
  public void deserialize(ByteBuf input) {
    seqId = ByteBufSerdeUtils.deserializeInt(input);
    methodId = ByteBufSerdeUtils.deserializeInt(input);
    state = ServerState.valueOf(ByteBufSerdeUtils.deserializeInt(input));
    responseType = ResponseType.valueOf(ByteBufSerdeUtils.deserializeInt(input));
    detail = ByteBufSerdeUtils.deserializeUTF8(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.INT_LENGTH * 4 + ByteBufSerdeUtils.serializedUTF8Len(detail);
  }

  public int getSeqId() {
    return seqId;
  }

  public void setSeqId(int seqId) {
    this.seqId = seqId;
  }

  public ServerState getState() {
    return state;
  }

  public void setState(ServerState state) {
    this.state = state;
  }

  public ResponseType getResponseType() {
    return responseType;
  }

  public void setResponseType(ResponseType responseType) {
    this.responseType = responseType;
  }

  public String getDetail() {
    return detail;
  }

  public void setDetail(String detail) {
    this.detail = detail;
  }

  @Override
  public String toString() {
    return "ResponseHeader{" +
        "seqId=" + seqId +
        ", state=" + state +
        ", responseType=" + responseType +
        ", detail='" + detail + '\'' +
        '}';
  }

  public int getMethodId() {
    return methodId;
  }

  public void setMethodId(int methodId) {
    this.methodId = methodId;
  }
}
