package com.tencent.angel.psagent.matrix.transport;

public class UserRequestStageTimes {
  public String funcName;
  public long splitTime;
  public long sendTime;
  public long mergeTime;
  public int mergeCounter = 1;

  @Override
  public String toString() {
    return "UserRequestStageTimes{" +
        "funcName='" + funcName + '\'' +
        ", splitTime=" + splitTime +
        ", sendTime=" + sendTime +
        ", mergeTime=" + mergeTime +
        ", mergeCounter=" + mergeCounter +
        '}';
  }

  public void merge(UserRequestStageTimes userRequestTimes) {
    this.splitTime += userRequestTimes.splitTime;
    this.sendTime += userRequestTimes.sendTime;
    this.mergeTime += userRequestTimes.mergeTime;
    mergeCounter++;
  }
}
