package com.tencent.angel.ps.server.data.profiling;

public class UserRequestTimes {
  public volatile long start;
  public volatile long scheduleStart;
  public volatile long splitStart;
  public volatile long mergeStart;
  public volatile long mergeEnd;
}
