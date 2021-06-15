package com.tencent.angel.psagent.matrix.transport.response;

import java.util.concurrent.atomic.AtomicInteger;

public abstract class ResponseCache implements IResponseCache {
  protected final int expectedResponseNum;
  protected final AtomicInteger receivedResponseNum = new AtomicInteger(0);

  public ResponseCache(int size) {
    this.expectedResponseNum = size;
  }

  public int getExpectedResponseNum() {
    return expectedResponseNum;
  }
}
