package com.tencent.angel.ps.server.data.handler;

import com.tencent.angel.ps.server.data.request.RequestData;
import com.tencent.angel.ps.server.data.request.RequestHeader;
import com.tencent.angel.ps.server.data.response.ResponseData;
import io.netty.buffer.ByteBuf;

public interface IHandler {
  RequestData parseRequest(ByteBuf in);
  ResponseData handle(RequestHeader header, RequestData request) throws Exception;
}
