package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;
import java.util.concurrent.ConcurrentHashMap;

public class MapResponseCache extends ResponseCache {
  private final ConcurrentHashMap<Request, Response> responses;
  public MapResponseCache(int size) {
    super(size);
    responses = new ConcurrentHashMap<>(size);
  }

  @Override
  public void add(Request request, Response response) {
    responses.put(request, response);
  }

  @Override
  public boolean canMerge() {
    return responses.size() >= expectedResponseNum;
  }

  @Override
  public void clear() {
    responses.clear();
  }

  public ConcurrentHashMap<Request, Response> getResponses() {
    return responses;
  }
}
