package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;
import io.netty.buffer.ByteBuf;

public interface IStreamHandler {
  Request parseRequest(ByteBuf in);
  ByteBuf handle(ByteBuf in);
  Response handleError(Throwable exp);
  Response generateResponse();
}
