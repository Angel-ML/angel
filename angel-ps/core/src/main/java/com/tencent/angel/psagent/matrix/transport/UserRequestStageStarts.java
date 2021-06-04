package com.tencent.angel.psagent.matrix.transport;

public class UserRequestStageStarts {
  public volatile long splitStart;
  public volatile long sendStart;
  public volatile long mergeStart;
  public volatile long mergeOver;
}
