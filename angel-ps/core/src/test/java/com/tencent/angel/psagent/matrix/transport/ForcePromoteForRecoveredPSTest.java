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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.request.RequestContext;
import java.util.Collection;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Test;

/**
 * Tests for the {@code REFRESH_SERVER_LOCATION_SUCCESS}-driven force-promotion of parked
 * requests on a freshly recovered PS. Targets {@link
 * MatrixTransportClient#forcePromoteForRecoveredPSImpl(PriorityQueue, LinkedBlockingQueue,
 * ParameterServerId)} which is the package-private helper that ignores remaining back-off
 * and pulls every request belonging to the recovered PS out of the failed-cache heap into
 * the schedulable queue.
 *
 * <p>The contract under test:
 * <ul>
 *   <li>requests for OTHER PSes must remain in the failed cache untouched;</li>
 *   <li>the entire matched set must move atomically — none left behind;</li>
 *   <li>back-off is intentionally bypassed: even requests whose nextRetryTs is well in
 *       the future move immediately;</li>
 *   <li>null recoveredServerId / empty queue are no-ops.</li>
 * </ul>
 */
public class ForcePromoteForRecoveredPSTest {

  private static Request requestForPS(int psIndex, long nextRetryTs) {
    Request r = new Request();
    RequestContext ctx = new RequestContext();
    ctx.setServerId(new ParameterServerId(psIndex));
    ctx.setNextRetryTs(nextRetryTs);
    r.setContext(ctx);
    return r;
  }

  /** Heap ordered by nextRetryTs — same comparator the dispatcher uses. */
  private static PriorityQueue<Request> newHeap() {
    return new PriorityQueue<>(8, new Comparator<Request>() {
      @Override public int compare(Request a, Request b) {
        return Long.compare(
            a.getContext().getNextRetryTs(),
            b.getContext().getNextRetryTs());
      }
    });
  }

  /**
   * {@link Request#equals(Object)} is hard-coded to return {@code false}, so
   * {@link Collection#contains(Object)} is unusable. We compare by identity — which is
   * exactly the contract the production code relies on too.
   */
  private static boolean containsByRef(Iterable<Request> haystack, Request needle) {
    for (Request r : haystack) {
      if (r == needle) return true;
    }
    return false;
  }

  @Test
  public void promotesAllRequestsForRecoveredPS() {
    PriorityQueue<Request> from = newHeap();
    LinkedBlockingQueue<Request> to = new LinkedBlockingQueue<>();
    Request a = requestForPS(7, 1000L);
    Request b = requestForPS(7, 5000L);
    Request c = requestForPS(7, Long.MAX_VALUE);  // back-off in the far future
    from.add(a);
    from.add(b);
    from.add(c);

    int moved = MatrixTransportClient.forcePromoteForRecoveredPSImpl(
        from, to, new ParameterServerId(7));

    assertEquals(3, moved);
    assertTrue("source heap should be drained of PS-7 requests", from.isEmpty());
    assertEquals("all three requests must land in the schedulable queue",
        3, to.size());
    assertTrue(containsByRef(to, a));
    assertTrue(containsByRef(to, b));
    assertTrue(containsByRef(to, c));
  }

  @Test
  public void leavesRequestsForOtherPSesInPlace() {
    PriorityQueue<Request> from = newHeap();
    LinkedBlockingQueue<Request> to = new LinkedBlockingQueue<>();
    Request mine = requestForPS(7, 1000L);
    Request other1 = requestForPS(8, 1000L);
    Request other2 = requestForPS(9, 2000L);
    from.add(mine);
    from.add(other1);
    from.add(other2);

    int moved = MatrixTransportClient.forcePromoteForRecoveredPSImpl(
        from, to, new ParameterServerId(7));

    assertEquals(1, moved);
    assertEquals("only the target-PS request should move", 1, to.size());
    assertTrue(containsByRef(to, mine));
    assertFalse("other PSes' requests must remain in the failed cache",
        containsByRef(to, other1));
    assertFalse(containsByRef(to, other2));
    assertEquals("source heap must retain the unrelated requests", 2, from.size());
    assertTrue(containsByRef(from, other1));
    assertTrue(containsByRef(from, other2));
  }

  @Test
  public void emptyHeapIsNoop() {
    PriorityQueue<Request> from = newHeap();
    LinkedBlockingQueue<Request> to = new LinkedBlockingQueue<>();
    int moved = MatrixTransportClient.forcePromoteForRecoveredPSImpl(
        from, to, new ParameterServerId(7));
    assertEquals(0, moved);
    assertTrue(to.isEmpty());
    assertTrue(from.isEmpty());
  }

  @Test
  public void nullServerIdIsNoop() {
    PriorityQueue<Request> from = newHeap();
    LinkedBlockingQueue<Request> to = new LinkedBlockingQueue<>();
    Request r = requestForPS(7, 1000L);
    from.add(r);

    int moved = MatrixTransportClient.forcePromoteForRecoveredPSImpl(from, to, null);
    assertEquals(0, moved);
    assertEquals("the request must be left undisturbed when serverId is null",
        1, from.size());
    assertTrue(to.isEmpty());
  }

  @Test
  public void noMatchingRequestsLeavesEverythingAlone() {
    PriorityQueue<Request> from = newHeap();
    LinkedBlockingQueue<Request> to = new LinkedBlockingQueue<>();
    Request a = requestForPS(8, 1000L);
    Request b = requestForPS(9, 2000L);
    from.add(a);
    from.add(b);

    int moved = MatrixTransportClient.forcePromoteForRecoveredPSImpl(
        from, to, new ParameterServerId(7));
    assertEquals(0, moved);
    assertEquals(2, from.size());
    assertTrue(to.isEmpty());
  }
}
