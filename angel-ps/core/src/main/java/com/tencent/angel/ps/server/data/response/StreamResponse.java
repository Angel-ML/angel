package com.tencent.angel.ps.server.data.response;

import io.netty.buffer.ByteBuf;

public class StreamResponse extends Response {
  private final ByteBuf out;

  public StreamResponse(ByteBuf out) {
    this.out = out;
  }

  public ByteBuf getOut() {
    return out;
  }
}
