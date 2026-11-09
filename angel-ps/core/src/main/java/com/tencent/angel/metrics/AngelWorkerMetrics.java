package com.tencent.angel.metrics;

import java.util.concurrent.atomic.AtomicLong;

/**
 * collect angel psf metrics
 */
public class AngelWorkerMetrics extends AngelMetrics {

  private final long timestamp;
  private  String appId;//driver applicationId
  private final String jvmTaskId;//driver or executor containerId
  private final String psfName;//ps func name
  private final AtomicLong reqSchedWaitTime = new AtomicLong(0);
  private final AtomicLong reqChannelWaitTime = new AtomicLong(0);
  private final AtomicLong reqSerializeAllocTime = new AtomicLong(0);
  private final AtomicLong reqSerializeTime = new AtomicLong(0);
  private final AtomicLong reqSendTime = new AtomicLong(0);
  private final AtomicLong reqHandleWaitTime = new AtomicLong(0);
  private final AtomicLong reqDeserializeTime = new AtomicLong(0);
  private final AtomicLong reqHandleTime = new AtomicLong(0);
  private final AtomicLong resSerializeAllocTime = new AtomicLong(0);
  private final AtomicLong resSerializeTime = new AtomicLong(0);
  private final AtomicLong resSendTime = new AtomicLong(0);
  private final AtomicLong resDeserializeTime = new AtomicLong(0);
  private final AtomicLong resHandleTime = new AtomicLong(0);
  private final AtomicLong psfAvgMsgSize = new AtomicLong(0);
  private String psfFailCnt = "";
  private String psfFailMsg = "";

  public AngelWorkerMetrics(String appId, String jvmTaskId, AngelWorkerLoad workerLoad) {
    this.timestamp = System.currentTimeMillis();
    this.appId = appId;
    this.jvmTaskId = jvmTaskId;
    this.psfName = workerLoad.getPsfName();
  }

  @Override
  public String getAppId() {
    return null;
  }

  public void setAppId(String appId) {
    this.appId = appId;
  }

  @Override
  public void merge(MetricsItem metrics) {
    AngelWorkerLoad workerMetrics = (AngelWorkerLoad) metrics;
    this.reqSchedWaitTime.addAndGet(workerMetrics.getReqSchedWaitTime());
    this.reqChannelWaitTime.addAndGet(workerMetrics.getReqChannelWaitTime());
    this.reqSerializeAllocTime.addAndGet(workerMetrics.getReqSerializeAllocTime());
    this.reqSerializeTime.addAndGet(workerMetrics.getReqSerializeTime());
    this.reqSendTime.addAndGet(workerMetrics.getReqSendTime());
    this.reqHandleWaitTime.addAndGet(workerMetrics.getReqHandleWaitTime());
    this.reqDeserializeTime.addAndGet(workerMetrics.getReqDeserializeTime());
    this.reqHandleTime.addAndGet(workerMetrics.getReqHandleTime());
    this.resSerializeAllocTime.addAndGet(workerMetrics.getResSerializeAllocTime());
    this.resSerializeTime.addAndGet(workerMetrics.getResSerializeTime());
    this.resSendTime.addAndGet(workerMetrics.getResSendTime());
    this.resDeserializeTime.addAndGet(workerMetrics.getResDeserializeTime());
    this.resHandleTime.addAndGet(workerMetrics.getResHandleTime());
    this.psfAvgMsgSize.addAndGet(workerMetrics.getPsfAvgMsgSize());
    counter.incrementAndGet();
  }

  @Override
  public String generateTextMessage(String sep) {
    StringBuilder stringBuilder = new StringBuilder();
    stringBuilder.append(timestamp);
    stringBuilder.append(sep);
    stringBuilder.append(appId);
    stringBuilder.append(sep);
    stringBuilder.append(jvmTaskId);
    stringBuilder.append(sep);
    stringBuilder.append(psfName);
    stringBuilder.append(sep);
    stringBuilder.append(reqSchedWaitTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqChannelWaitTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqSerializeAllocTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqSerializeTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqSendTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqHandleWaitTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqDeserializeTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(reqHandleTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(resSerializeAllocTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(resSerializeTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(resSendTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(resDeserializeTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(resHandleTime.get());
    stringBuilder.append(sep);
    stringBuilder.append(psfAvgMsgSize.get());
    stringBuilder.append(sep);
    stringBuilder.append(psfFailCnt);
    stringBuilder.append(sep);
    stringBuilder.append(psfFailMsg);
    stringBuilder.append(sep);
    stringBuilder.append(counter.get());

    return stringBuilder.toString();
  }

  public void setPsfFailCnt(String psfFailCnt) {
    this.psfFailCnt = psfFailCnt;
  }

  public void setPsfFailMsg(String psfFailMsg) {
    this.psfFailMsg = psfFailMsg;
  }

}
