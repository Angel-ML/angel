package com.tencent.angel.metrics;

import com.tencent.angel.psagent.matrix.transport.PSFTimes;

public class AngelWorkerLoad extends MetricsItem {
  private final String psfName;//ps func name
  private final long reqSchedWaitTime;
  private final long reqChannelWaitTime;
  private final long reqSerializeAllocTime;
  private final long reqSerializeTime;
  private final long reqSendTime;
  private final long reqHandleWaitTime;
  private final long reqDeserializeTime;
  private final long reqHandleTime;
  private final long resSerializeAllocTime;
  private final long resSerializeTime;
  private final long resSendTime;
  private final long resDeserializeTime;
  private final long resHandleTime;
  private final long psfAvgMsgSize;

  public AngelWorkerLoad(String psfName) {
    this.psfName = psfName;
    this.reqSchedWaitTime = 0;
    this.reqChannelWaitTime = 0;
    this.reqSerializeAllocTime = 0;
    this.reqSerializeTime = 0;
    this.reqSendTime = 0;
    this.reqHandleWaitTime = 0;
    this.reqDeserializeTime = 0;
    this.reqHandleTime = 0;
    this.resSerializeAllocTime = 0;
    this.resSerializeTime = 0;
    this.resSendTime = 0;
    this.resDeserializeTime = 0;
    this.resHandleTime = 0;
    this.psfAvgMsgSize = 0;
  }

  public AngelWorkerLoad(PSFTimes time) {
    this.psfName = time.funcName;
    this.reqSchedWaitTime = time.reqScheTime;
    this.reqChannelWaitTime = time.reqWaitChannelTime;
    this.reqSerializeAllocTime = time.reqBufAllocTime;
    this.reqSerializeTime = time.reqSerializeTime;
    this.reqSendTime = time.reqSendTime;
    this.reqHandleWaitTime = time.reqWaitScheTimePS;
    this.reqDeserializeTime = time.reqDeserializeTime;
    this.reqHandleTime = time.handleTime;
    this.resSerializeAllocTime = time.retBufAllocTime;
    this.resSerializeTime = time.retSerializeTime;
    this.resSendTime = time.retSendTime;
    this.resDeserializeTime = time.retDeserializeTime;
    this.resHandleTime = time.retHandleScheTime;
    this.psfAvgMsgSize = time.bufferLen;
  }

  @Override
  public String generateKey(String sep) {
    return null;
  }

  public String getPsfName() {
    return psfName;
  }

  public long getReqSchedWaitTime() {
    return reqSchedWaitTime;
  }

  public long getReqChannelWaitTime() {
    return reqChannelWaitTime;
  }

  public long getReqSerializeAllocTime() {
    return reqSerializeAllocTime;
  }

  public long getReqSerializeTime() {
    return reqSerializeTime;
  }

  public long getReqSendTime() {
    return reqSendTime;
  }

  public long getReqHandleWaitTime() {
    return reqHandleWaitTime;
  }

  public long getReqDeserializeTime() {
    return reqDeserializeTime;
  }

  public long getReqHandleTime() {
    return reqHandleTime;
  }

  public long getResSerializeAllocTime() {
    return resSerializeAllocTime;
  }

  public long getResSerializeTime() {
    return resSerializeTime;
  }

  public long getResSendTime() {
    return resSendTime;
  }

  public long getResDeserializeTime() {
    return resDeserializeTime;
  }

  public long getResHandleTime() {
    return resHandleTime;
  }

  public long getPsfAvgMsgSize() {
    return psfAvgMsgSize;
  }

  @Override
  public String toString() {
    return "AngelWorkerLoad{" +
        "psfName='" + psfName + '\'' +
        ", reqSchedWaitTime=" + reqSchedWaitTime +
        ", reqChannelWaitTime=" + reqChannelWaitTime +
        ", reqSerializeAllocTime=" + reqSerializeAllocTime +
        ", reqSerializeTime=" + reqSerializeTime +
        ", reqSendTime=" + reqSendTime +
        ", reqHandleWaitTime=" + reqHandleWaitTime +
        ", reqDeserializeTime=" + reqDeserializeTime +
        ", reqHandleTime=" + reqHandleTime +
        ", resSerializeAllocTime=" + resSerializeAllocTime +
        ", resSerializeTime=" + resSerializeTime +
        ", resSendTime=" + resSendTime +
        ", resDeserializeTime=" + resDeserializeTime +
        ", resHandleTime=" + resHandleTime +
        ", psfAvgMsgSize=" + psfAvgMsgSize +
        '}';
  }
}
