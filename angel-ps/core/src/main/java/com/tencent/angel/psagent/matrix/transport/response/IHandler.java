package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import io.netty.buffer.ByteBuf;

public interface IHandler {
  ResponseData parseResponse(ByteBuf in);
  void handle(FutureResult finalResult, UserRequest userRequest, ResponseCache responseCache);
}
