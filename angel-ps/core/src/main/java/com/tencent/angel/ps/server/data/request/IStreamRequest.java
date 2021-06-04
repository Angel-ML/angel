package com.tencent.angel.ps.server.data.request;

import io.netty.buffer.ByteBuf;

public interface IStreamRequest {
  void deserializeHeader(ByteBuf in);
  ByteBuf getInputBuffer();
}
