package com.tencent.angel.metrics;

public class AngelPSLoad extends MetricsItem {

  private final String serverId;
  private final String reqFuncName;
  private final long reqPartKeyId;
  private final long reqBufferLen;
  private final long resBufferLen;
  private final long dataBufferLen;

  public AngelPSLoad(String serverId, String reqFuncName, long reqPartKeyId, long reqBufferLen,
      long resBufferLen, long dataBufferLen) {
    this.serverId = serverId;
    this.reqFuncName = reqFuncName;
    this.reqPartKeyId = reqPartKeyId;
    this.reqBufferLen = reqBufferLen;
    this.resBufferLen = resBufferLen;
    this.dataBufferLen = dataBufferLen;
  }

  public String getReqFuncName() {
    return reqFuncName;
  }

  public long getReqPartKeyId() {
    return reqPartKeyId;
  }

  public long getReqBufferLen() {
    return reqBufferLen;
  }

  public long getResBufferLen() {
    return resBufferLen;
  }

  public long getDataBufferLen() {
    return dataBufferLen;
  }

  @Override
  public String generateKey(String sep) {
    StringBuilder sb = new StringBuilder();
    sb.append(reqPartKeyId);
    sb.append(serverId);
    sb.append(reqFuncName);
    return sb.toString();
  }

  public String getServerId() {
    return serverId;
  }

  @Override
  public String toString() {
    return "AngelPSLoad{" +
        "serverId='" + serverId + '\'' +
        ", reqFuncName='" + reqFuncName + '\'' +
        ", reqPartKeyId=" + reqPartKeyId +
        ", reqBufferLen=" + reqBufferLen +
        ", resBufferLen=" + resBufferLen +
        ", dataBufferLen=" + dataBufferLen +
        '}';
  }
}
