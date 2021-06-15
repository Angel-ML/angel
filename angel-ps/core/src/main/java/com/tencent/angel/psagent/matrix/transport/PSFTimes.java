package com.tencent.angel.psagent.matrix.transport;

public class PSFTimes {
  public String funcName;
  public long reqScheTime;
  public long reqWaitChannelTime;
  public long getTokenTime;
  public long reqBufAllocTime;
  public long reqSerializeTime;
  public long reqSendTime;
  public long reqWaitScheTimePS;
  public long reqDeserializeTime;
  public long handleTime;
  public long retBufAllocTime;
  public long retSerializeTime;
  public long retSendScheTime;
  public long retWaitChannelTime;
  public long retSendTime;
  public long retHandleScheTime;
  public long retDeserializeTime;

  @Override
  public String toString() {
    return "PSFTimes{" +
        "funcName='" + funcName + '\'' +
        ", reqScheTime=" + reqScheTime +
        ", reqWaitChannelTime=" + reqWaitChannelTime +
        ", getTokenTime=" + getTokenTime +
        ", reqBufAllocTime=" + reqBufAllocTime +
        ", reqSerializeTime=" + reqSerializeTime +
        ", reqSendTime=" + reqSendTime +
        ", reqWaitScheTimePS=" + reqWaitScheTimePS +
        ", reqDeserializeTime=" + reqDeserializeTime +
        ", handleTime=" + handleTime +
        ", retBufAllocTime=" + retBufAllocTime +
        ", retSerializeTime=" + retSerializeTime +
        ", retSendScheTime=" + retSendScheTime +
        ", retWaitChannelTime=" + retWaitChannelTime +
        ", retSendTime=" + retSendTime +
        ", retHandleScheTime=" + retHandleScheTime +
        ", retDeserializeTime=" + retDeserializeTime +
        ", mergeConter=" + mergeConter +
        '}';
  }

  public int mergeConter = 1;

  public void merge(PSFTimes psfTimes) {
    this.reqScheTime += psfTimes.reqScheTime;
    this.reqWaitChannelTime += psfTimes.reqWaitChannelTime;
    this.getTokenTime += psfTimes.getTokenTime;
    this.reqBufAllocTime += psfTimes.reqBufAllocTime;
    this.reqSerializeTime += psfTimes.reqSerializeTime;
    this.reqSendTime += psfTimes.reqSendTime;
    this.reqWaitScheTimePS += psfTimes.reqWaitScheTimePS;
    this.reqDeserializeTime += psfTimes.reqDeserializeTime;
    this.handleTime += psfTimes.handleTime;
    this.retBufAllocTime += psfTimes.retBufAllocTime;
    this.retSerializeTime += psfTimes.retSerializeTime;
    this.retSendScheTime += psfTimes.retSendScheTime;
    this.retWaitChannelTime += psfTimes.retWaitChannelTime;
    this.retSendTime += psfTimes.retSendTime;
    this.retHandleScheTime += psfTimes.retHandleScheTime;
    this.retDeserializeTime += psfTimes.retDeserializeTime;

    mergeConter++;
  }
}
