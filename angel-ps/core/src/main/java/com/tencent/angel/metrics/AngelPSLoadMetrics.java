package com.tencent.angel.metrics;

import java.util.concurrent.atomic.AtomicLong;

public class AngelPSLoadMetrics extends AngelMetrics {

  private final long timestamp;
  private final String jvmTaskId;//ps containerId
  private final String serverId;
  private final String reqFuncName;
  private final String resFuncName;
  private final long reqPartKeyId;
  private final long resPartKeyId;
  private final AtomicLong reqBufferLen = new AtomicLong(0);
  private final AtomicLong resBufferLen = new AtomicLong(0);
  private final AtomicLong dataBufferLen = new AtomicLong(0);

  private final long totalRPCCounter;
  private final String psState;

  public AngelPSLoadMetrics(String jvmTaskId, long totalRPCCounter, String psState, AngelPSLoad metricsItem) {
    this.timestamp = System.currentTimeMillis();
    this.jvmTaskId = jvmTaskId;
    this.serverId = metricsItem.getServerId();
    this.reqFuncName = metricsItem.getReqFuncName();
    this.resFuncName = "";
    this.reqPartKeyId = metricsItem.getReqPartKeyId();
    this.resPartKeyId = metricsItem.getReqPartKeyId();
    this.totalRPCCounter = totalRPCCounter;
    this.psState = psState;
  }

  @Override
  public String getAppId() {
    return null;
  }

  @Override
  public void merge(MetricsItem metrics) {
    AngelPSLoad newMetrics = (AngelPSLoad) metrics;
    reqBufferLen.addAndGet(newMetrics.getReqBufferLen());
    resBufferLen.addAndGet(newMetrics.getResBufferLen());
    dataBufferLen.addAndGet(newMetrics.getDataBufferLen());
    counter.incrementAndGet();
  }

  @Override
  public String generateTextMessage(String sep) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(timestamp);
    stringBuilder.append(sep);
    stringBuilder.append(jvmTaskId);
    stringBuilder.append(sep);
    stringBuilder.append(serverId);
    stringBuilder.append(sep);
    stringBuilder.append(reqFuncName);
    stringBuilder.append(sep);
    stringBuilder.append(resFuncName);
    stringBuilder.append(sep);
    stringBuilder.append(reqPartKeyId);
    stringBuilder.append(sep);
    stringBuilder.append(resPartKeyId);
    stringBuilder.append(sep);
    stringBuilder.append(reqBufferLen);
    stringBuilder.append(sep);
    stringBuilder.append(resBufferLen);
    stringBuilder.append(sep);
    stringBuilder.append(counter.get());
    stringBuilder.append(sep);
    stringBuilder.append(totalRPCCounter);
    stringBuilder.append(sep);
    stringBuilder.append(psState);
    stringBuilder.append(sep);
    stringBuilder.append(dataBufferLen);

    return stringBuilder.toString();
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getJvmTaskId() {
    return jvmTaskId;
  }

  public String getServerId() {
    return serverId;
  }

  public String getReqFuncName() {
    return reqFuncName;
  }

  public String getResFuncName() {
    return resFuncName;
  }

  public long getReqPartKeyId() {
    return reqPartKeyId;
  }

  public long getResPartKeyId() {
    return resPartKeyId;
  }

  public long getReqBufferLen() {
    return reqBufferLen.get();
  }

  public long getResBufferLen() {
    return resBufferLen.get();
  }

  public long getDataBufferLen() {
    return dataBufferLen.get();
  }

  public long getTotalRPCCounter() {
    return totalRPCCounter;
  }

  public String getPsState() {
    return psState;
  }
}
