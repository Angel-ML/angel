/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 */
package com.tencent.angel.psagent.matrix.transport.response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Verifies the O(1) readiness gate and duplicate-suppression behaviour added to
 * {@link MapResponseCache}.
 */
public class MapResponseCacheTest {

  /**
   * Two distinct {@link Request} instances are unconditionally treated as different keys
   * (each carries its own auto-incremented hashCode and the type explicitly returns
   * {@code false} from {@code equals}). Adding {@code expectedResponseNum} of them must
   * flip {@code canMerge()} exactly once.
   */
  @Test public void canMerge_flipsExactlyOnce_whenDistinctRequestsArrive() {
    MapResponseCache cache = new MapResponseCache(3);
    assertFalse(cache.canMerge());

    cache.add(new Request(), new Response());
    assertFalse(cache.canMerge());

    cache.add(new Request(), new Response());
    assertFalse(cache.canMerge());

    cache.add(new Request(), new Response());
    assertTrue(cache.canMerge());

    // Extra arrivals (e.g. a late duplicate) must not flip canMerge back.
    cache.add(new Request(), new Response());
    assertTrue(cache.canMerge());
  }

  /**
   * The same {@link Request} reference added twice must be ignored on the second call so
   * that a netty-layer re-delivery cannot inflate the counter past
   * {@code expectedResponseNum} and trip {@code canMerge} prematurely.
   */
  @Test public void duplicateSameRequestReference_isIgnored() {
    MapResponseCache cache = new MapResponseCache(2);

    Request r1 = new Request();
    Request r2 = new Request();

    cache.add(r1, new Response());
    cache.add(r1, new Response()); // duplicate -- must be ignored
    assertFalse("one logical part received -> not yet mergeable", cache.canMerge());

    cache.add(r2, new Response());
    assertTrue(cache.canMerge());
    assertEquals(2, cache.getResponses().size());
  }

  /** clear() must reset both the response map and the counter so that the cache is reusable. */
  @Test public void clear_resetsCounterAndMap() {
    MapResponseCache cache = new MapResponseCache(2);
    cache.add(new Request(), new Response());
    cache.add(new Request(), new Response());
    assertTrue(cache.canMerge());

    cache.clear();
    assertFalse(cache.canMerge());
    assertEquals(0, cache.getResponses().size());

    // Reusing after clear() must work as if freshly constructed.
    cache.add(new Request(), new Response());
    assertFalse(cache.canMerge());
    cache.add(new Request(), new Response());
    assertTrue(cache.canMerge());
  }

  /**
   * Under concurrent adders with unique {@link Request} instances, {@code canMerge} must
   * become {@code true} exactly when {@code expectedResponseNum} distinct adds have
   * completed. This guards against torn counter updates.
   */
  @Test public void canMerge_isAccurateUnderConcurrentAdds() throws Exception {
    final int n = 200;
    final MapResponseCache cache = new MapResponseCache(n);
    final ExecutorService pool = Executors.newFixedThreadPool(16);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Request> reqs = new ArrayList<>(n);
    for (int i = 0; i < n; i++) reqs.add(new Request());

    try {
      for (int i = 0; i < n; i++) {
        final Request r = reqs.get(i);
        pool.submit(() -> {
          try {
            start.await();
          } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
          }
          cache.add(r, new Response());
        });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
    } finally {
      pool.shutdownNow();
    }

    assertTrue(cache.canMerge());
    assertEquals(n, cache.getResponses().size());
  }
}
