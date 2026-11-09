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
package com.tencent.angel.psagent.matrix.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.RequestContext;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.junit.Test;

/**
 * Verifies the ordering semantics that {@code MatrixTransportClient.RequestDispatcher} relies
 * on after the {@code LinkedList → PriorityQueue<Request>} switch for {@code failedGetCache}
 * / {@code failedPutCache}. The dispatcher's correctness hinges on:
 *
 * <ol>
 *   <li>{@code peek()} always returns the entry with the smallest {@code nextRetryTs};</li>
 *   <li>{@code poll()} preserves this invariant after removing the head;</li>
 *   <li>{@code addAll(bypass)} after polling correctly reinserts the bypassed entries.</li>
 * </ol>
 *
 * <p>The comparator under test is the same one used inside {@code RequestDispatcher}, just
 * inlined here so we don't have to expose the private static field.</p>
 */
public class FailedRequestPriorityOrderTest {

  private static final Comparator<Request> BY_NEXT_RETRY_TS =
      new Comparator<Request>() {
        @Override public int compare(Request a, Request b) {
          return Long.compare(
              a.getContext().getNextRetryTs(),
              b.getContext().getNextRetryTs());
        }
      };

  private static Request reqAt(long ts) {
    RequestContext c = new RequestContext();
    c.setNextRetryTs(ts);
    Request r = new Request();
    r.setContext(c);
    return r;
  }

  @Test
  public void peekReturnsEarliestRetryTs() {
    PriorityQueue<Request> q = new PriorityQueue<>(BY_NEXT_RETRY_TS);
    q.add(reqAt(100));
    q.add(reqAt(50));
    q.add(reqAt(200));
    assertEquals(50L, q.peek().getContext().getNextRetryTs());
  }

  @Test
  public void pollDrainsInAscendingOrder() {
    PriorityQueue<Request> q = new PriorityQueue<>(BY_NEXT_RETRY_TS);
    long[] inserts = {500, 100, 300, 200, 400};
    for (long t : inserts) q.add(reqAt(t));
    long prev = Long.MIN_VALUE;
    while (!q.isEmpty()) {
      long ts = q.poll().getContext().getNextRetryTs();
      if (ts < prev) {
        org.junit.Assert.fail("poll order violation: " + prev + " before " + ts);
      }
      prev = ts;
    }
  }

  /**
   * Simulates the "expired but PS not schedulable → bypass list → reinsert" pattern in
   * {@code RequestDispatcher.drainExpiredFailedRequests}. After reinserting bypassed
   * entries the queue must still expose them in the correct order on subsequent peeks.
   */
  @Test
  public void bypassedEntriesReinsertCleanly() {
    PriorityQueue<Request> q = new PriorityQueue<>(BY_NEXT_RETRY_TS);
    q.add(reqAt(100));
    q.add(reqAt(200));
    q.add(reqAt(300));

    Request bypassed = q.poll();        // ts=100, simulate "PS gray, hold this aside"
    Request scheduled = q.poll();       // ts=200, simulate "scheduled"
    assertNotNull(bypassed);
    assertNotNull(scheduled);
    assertEquals(100L, bypassed.getContext().getNextRetryTs());
    assertEquals(200L, scheduled.getContext().getNextRetryTs());

    // Re-insert bypassed; the next peek should be the bypassed-100, not the remaining-300.
    q.addAll(java.util.Collections.singletonList(bypassed));
    assertEquals(100L, q.peek().getContext().getNextRetryTs());

    // Drain and verify final order.
    assertEquals(100L, q.poll().getContext().getNextRetryTs());
    assertEquals(300L, q.poll().getContext().getNextRetryTs());
    assertNull(q.poll());
  }

  @Test
  public void equalRetryTsIsHandledStably() {
    PriorityQueue<Request> q = new PriorityQueue<>(BY_NEXT_RETRY_TS);
    Request a = reqAt(100);
    Request b = reqAt(100);
    Request c = reqAt(100);
    q.add(a); q.add(b); q.add(c);

    // We don't care about ordering between equal keys, only that all 3 come out and that
    // peek/poll never returns something with > 100 while these are present.
    int drained = 0;
    while (!q.isEmpty()) {
      Request r = q.poll();
      assertEquals(100L, r.getContext().getNextRetryTs());
      drained++;
    }
    assertEquals(3, drained);
  }
}
