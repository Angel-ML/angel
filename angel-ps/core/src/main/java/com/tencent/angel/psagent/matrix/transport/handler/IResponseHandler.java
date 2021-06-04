package com.tencent.angel.psagent.matrix.transport.handler;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseHeader;

public interface IResponseHandler {
  ResponseData parseResponse();
  void handle(Request reqeust, ResponseHeader header, ResponseData data);
}
