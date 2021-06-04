package com.tencent.angel.ps.server.data.profiling;

public class ClientTimes {
  public volatile long requestStart = 0;
  public volatile long requestScheduleStart = 0;
  public volatile long getChannelStart = 0;
  public volatile long getTokenStart = 0;
  public volatile long allocateRequestBufStart = 0;
  public volatile long serializeRequestStart = 0;
  public volatile long sendRequestStart = 0;
  public volatile long sendRequestOver = 0;

  public volatile long recvResponseStart = 0;
  public volatile long deserializeResponseStart = 0;
  public volatile long handleResponseStart = 0;
  public volatile long mergeStart = 0;
}
