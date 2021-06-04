package com.tencent.angel.psagent.matrix.transport;

import io.netty.buffer.ByteBuf;

public class SerializedMsgWithTime {
  public ByteBuf msg;
  public long ts;

  public SerializedMsgWithTime(ByteBuf msg) {
    this.msg = msg;
    this.ts = System.currentTimeMillis();
  }
}
