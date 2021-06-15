package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;

public interface IResponseCache {
  void add(Request request, Response response);
  boolean canMerge();
  void clear();
}
