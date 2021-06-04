package com.tencent.angel.ps.server.data.response;

import io.netty.buffer.ByteBuf;

public interface IStreamResponse {
  ByteBuf getOutputBuffer();
  void setOutputBuffer(ByteBuf out);
}
