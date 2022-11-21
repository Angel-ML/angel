package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

public class RequestHeader implements Serialize {
  public int clientId;
  public int token;
  public int userRequestId;
  public int seqId;
  public int methodId;
  public int matrixId;
  public int partId;
  public int handleElemNum;
  public int tryCount;

  public RequestHeader() {
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, clientId);
    ByteBufSerdeUtils.serializeInt(output, token);
    ByteBufSerdeUtils.serializeInt(output, userRequestId);
    ByteBufSerdeUtils.serializeInt(output, seqId);
    ByteBufSerdeUtils.serializeInt(output, methodId);
    ByteBufSerdeUtils.serializeInt(output, matrixId);
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, handleElemNum);
    ByteBufSerdeUtils.serializeInt(output, tryCount);
  }

  @Override
  public void deserialize(ByteBuf input) {
    clientId = ByteBufSerdeUtils.deserializeInt(input);
    token = ByteBufSerdeUtils.deserializeInt(input);
    userRequestId = ByteBufSerdeUtils.deserializeInt(input);
    seqId = ByteBufSerdeUtils.deserializeInt(input);
    methodId = ByteBufSerdeUtils.deserializeInt(input);
    matrixId = ByteBufSerdeUtils.deserializeInt(input);
    partId = ByteBufSerdeUtils.deserializeInt(input);
    handleElemNum = ByteBufSerdeUtils.deserializeInt(input);
    tryCount = ByteBufSerdeUtils.deserializeInt(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.INT_LENGTH * 9;
  }

  @Override
  public String toString() {
    return "RequestHeader{" +
        "clientId=" + clientId +
        ", token=" + token +
        ", userRequestId=" + userRequestId +
        ", seqId=" + seqId +
        ", methodId=" + methodId +
        ", matrixId=" + matrixId +
        ", partId=" + partId +
        ", handleElemNum=" + handleElemNum +
        ", tryCount=" + tryCount +
        '}';
  }

  public int getClientId() {
    return clientId;
  }

  public void setClientId(int clientId) {
    this.clientId = clientId;
  }

  public int getToken() {
    return token;
  }

  public void setToken(int token) {
    this.token = token;
  }

  public int getUserRequestId() {
    return userRequestId;
  }

  public void setUserRequestId(int userRequestId) {
    this.userRequestId = userRequestId;
  }

  public int getSeqId() {
    return seqId;
  }

  public void setSeqId(int seqId) {
    this.seqId = seqId;
  }

  public int getMethodId() {
    return methodId;
  }

  public void setMethodId(int methodId) {
    this.methodId = methodId;
  }

  public int getMatrixId() {
    return matrixId;
  }

  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  public int getPartId() {
    return partId;
  }

  public void setPartId(int partId) {
    this.partId = partId;
  }

  public int getHandleElemNum() {
    return handleElemNum;
  }

  public void setHandleElemNum(int handleElemNum) {
    this.handleElemNum = handleElemNum;
  }

  public int getTryCount() { return tryCount; }

  public void setTryCount(int tryCount) { this.tryCount = tryCount; }
}
