package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Cache of partition-level responses for a single user request.
 *
 * <p>{@link #canMerge()} historically used {@code responses.size() >= expectedResponseNum}, which
 * is O(n) for {@link ConcurrentHashMap} and -- more importantly -- can misbehave when the same
 * partition request is retried and a stale response is replaced in the map (size stays the same
 * across the put, so the size-based gate works only because put() of an identical key replaces
 * the value rather than incrementing size). To make the readiness signal both O(1) and immune to
 * duplicate deliveries we drive a dedicated {@code receivedResponseNum} counter, and ignore
 * duplicate puts for the same request key.</p>
 */
public class MapResponseCache extends ResponseCache {
  private final ConcurrentHashMap<Request, Response> responses;
  public MapResponseCache(int size) {
    super(size);
    responses = new ConcurrentHashMap<>(size);
  }

  @Override
  public void add(Request request, Response response) {
    if (responses.putIfAbsent(request, response) == null) {
      receivedResponseNum.incrementAndGet();
    }
  }

  @Override
  public boolean canMerge() {
    return receivedResponseNum.get() >= expectedResponseNum;
  }

  @Override
  public void clear() {
    responses.clear();
    receivedResponseNum.set(0);
  }

  public ConcurrentHashMap<Request, Response> getResponses() {
    return responses;
  }
}
