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
import static org.junit.Assert.assertTrue;

import com.tencent.angel.psagent.matrix.transport.MatrixTransportClient.GetChannelContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

/**
 * Unit tests for the {@link GetChannelContext} counter behaviour after the switch from
 * {@code int + ReentrantLock} to {@link java.util.concurrent.atomic.LongAdder}.
 *
 * <p>We are explicitly NOT testing lock-free atomicity of {@code reset()} versus concurrent
 * increments — that case is documented as "lost increments fold into the next window" and
 * the only consumer of these counters is the 1Hz periodic check, which doesn't care.</p>
 */
public class GetChannelContextTest {

  @Test
  public void newContextStartsAtZero() {
    GetChannelContext ctx = new GetChannelContext();
    assertEquals(0, ctx.getInactiveCounter());
    assertEquals(0, ctx.getSuccessCounter());
    assertEquals(0, ctx.getFailedCounter());
    assertTrue("lastCheckTs should be initialised", ctx.getLastCheckTs() > 0);
  }

  @Test
  public void singleThreadedIncrementsAreVisible() {
    GetChannelContext ctx = new GetChannelContext();
    ctx.getChannelSuccess();
    ctx.getChannelSuccess();
    ctx.getChannelFailed();
    ctx.channelNotactive();
    ctx.channelNotactive();
    ctx.channelNotactive();
    assertEquals(2, ctx.getSuccessCounter());
    assertEquals(1, ctx.getFailedCounter());
    assertEquals(3, ctx.getInactiveCounter());
  }

  @Test
  public void resetClearsCountsAndUpdatesTimestamp() throws InterruptedException {
    GetChannelContext ctx = new GetChannelContext();
    ctx.getChannelSuccess();
    ctx.channelNotactive();
    long beforeReset = ctx.getLastCheckTs();
    Thread.sleep(5);
    ctx.reset();
    assertEquals(0, ctx.getSuccessCounter());
    assertEquals(0, ctx.getFailedCounter());
    assertEquals(0, ctx.getInactiveCounter());
    assertTrue("reset should update lastCheckTs", ctx.getLastCheckTs() >= beforeReset);
  }

  /**
   * Cornerstone test: the whole point of moving to LongAdder is correctness under high
   * write contention WITHOUT the per-call lock. We hammer the three counters from many
   * threads and assert the sum matches the number of issued increments exactly.
   */
  @Test
  public void concurrentIncrementsAreNotLost() throws Exception {
    final int threads = 32;
    final int perThread = 20_000;
    GetChannelContext ctx = new GetChannelContext();
    ExecutorService pool = Executors.newFixedThreadPool(threads);
    CountDownLatch ready = new CountDownLatch(threads);
    CountDownLatch go = new CountDownLatch(1);

    for (int t = 0; t < threads; t++) {
      pool.submit(() -> {
        ready.countDown();
        try { go.await(); } catch (InterruptedException ignore) {}
        for (int i = 0; i < perThread; i++) {
          ctx.getChannelSuccess();
          ctx.getChannelFailed();
          ctx.channelNotactive();
        }
      });
    }

    ready.await();
    long startNs = System.nanoTime();
    go.countDown();
    pool.shutdown();
    assertTrue(pool.awaitTermination(15, TimeUnit.SECONDS));
    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;

    int expected = threads * perThread;
    assertEquals("success counter must be exact under contention",
        expected, ctx.getSuccessCounter());
    assertEquals("failed counter must be exact under contention",
        expected, ctx.getFailedCounter());
    assertEquals("inactive counter must be exact under contention",
        expected, ctx.getInactiveCounter());

    // Soft perf check: 32 * 20k * 3 = 1.92M atomic ops should comfortably finish in well
    // under the 15s test timeout on any modern CI box; if it ever takes more than 10s
    // something is wrong (e.g. accidentally reintroduced lock).
    assertTrue("LongAdder hot path took too long: " + elapsedMs + " ms", elapsedMs < 10_000);
  }

  /**
   * Verifies the saturation guard: if the LongAdder.sum() exceeds Integer.MAX_VALUE the
   * getter must return Integer.MAX_VALUE rather than wrap to a negative number. We can't
   * realistically push 2 billion increments through in a UT, so we use reflection to seed
   * the inner LongAdder directly.
   */
  @Test
  public void counterSaturatesAtIntegerMax() throws Exception {
    GetChannelContext ctx = new GetChannelContext();
    java.lang.reflect.Field f = GetChannelContext.class.getDeclaredField("successCounter");
    f.setAccessible(true);
    java.util.concurrent.atomic.LongAdder adder =
        (java.util.concurrent.atomic.LongAdder) f.get(ctx);
    adder.add((long) Integer.MAX_VALUE + 100L);
    assertEquals(Integer.MAX_VALUE, ctx.getSuccessCounter());
  }

  // -------- consecutiveInactiveWindows --------

  /**
   * Cross-window stuck-PS counter starts at zero and survives {@link
   * GetChannelContext#reset()}: that's the whole point — reset() clears the per-window
   * counters but the multi-window streak counter must be deliberately bumped or cleared.
   */
  @Test
  public void consecutiveInactiveWindowsStartsAtZeroAndSurvivesReset() {
    GetChannelContext ctx = new GetChannelContext();
    assertEquals(0, ctx.getConsecutiveInactiveWindows());

    ctx.incConsecutiveInactiveWindows();
    ctx.incConsecutiveInactiveWindows();
    assertEquals(2, ctx.getConsecutiveInactiveWindows());

    // reset() is the per-window action and must NOT touch the streak counter.
    ctx.channelNotactive();
    ctx.reset();
    assertEquals("reset() must not clear the cross-window counter",
        2, ctx.getConsecutiveInactiveWindows());
  }

  /**
   * The increment returns the post-increment value so the caller can branch on the
   * threshold without a separate read; the API contract explicitly exists for the
   * "consecutive >= 3" check site.
   */
  @Test
  public void incConsecutiveInactiveWindowsReturnsPostIncrement() {
    GetChannelContext ctx = new GetChannelContext();
    assertEquals(1, ctx.incConsecutiveInactiveWindows());
    assertEquals(2, ctx.incConsecutiveInactiveWindows());
    assertEquals(3, ctx.incConsecutiveInactiveWindows());
  }

  /**
   * clearConsecutiveInactiveWindows() must zero the streak even mid-flight, so that a
   * single window of forward progress on the PS hides any prior streak from the
   * STUCK_PS detector.
   */
  @Test
  public void clearConsecutiveInactiveWindowsZeroes() {
    GetChannelContext ctx = new GetChannelContext();
    ctx.incConsecutiveInactiveWindows();
    ctx.incConsecutiveInactiveWindows();
    ctx.incConsecutiveInactiveWindows();
    assertEquals(3, ctx.getConsecutiveInactiveWindows());
    ctx.clearConsecutiveInactiveWindows();
    assertEquals(0, ctx.getConsecutiveInactiveWindows());
  }
}
