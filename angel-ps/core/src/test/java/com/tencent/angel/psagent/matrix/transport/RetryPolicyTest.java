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

import com.tencent.angel.ps.server.data.response.ResponseType;
import org.junit.Test;

/**
 * Unit tests for {@link RetryPolicy}: the per-{@link ResponseType} back-off and try-limit
 * policy extracted from {@code MatrixTransportClient}. We assert the shape of the policy
 * (bounds, monotonicity, special-cases for FATAL / NETWORK) rather than exact values that
 * involve {@code ThreadLocalRandom}.
 */
public class RetryPolicyTest {

  private static final int RETRY_INTERVAL_MS = 10_000;
  private static final int BACKOFF_WINDOW_MS = 5_000;
  private static final int REQ_TIMEOUT_MS = 60_000;
  private static final int MAX_TRY = 10;

  private RetryPolicy policy() {
    return new RetryPolicy(RETRY_INTERVAL_MS, BACKOFF_WINDOW_MS, REQ_TIMEOUT_MS, MAX_TRY);
  }

  // ---- tryLimit -------------------------------------------------------------------------

  @Test
  public void tryLimitFatalIsOne() {
    RetryPolicy p = policy();
    assertEquals(1, p.tryLimit(ResponseType.SERVER_HANDLE_FATAL));
    assertEquals(1, p.tryLimit(ResponseType.UNSUPPORT_REQUEST));
    assertEquals(1, p.tryLimit(ResponseType.PARSE_HEADER_FAILED));
  }

  @Test
  public void tryLimitNetworkIsDoubleMaxTry() {
    RetryPolicy p = policy();
    assertEquals(2 * MAX_TRY, p.tryLimit(ResponseType.NETWORK_ERROR));
    assertEquals(2 * MAX_TRY, p.tryLimit(ResponseType.CONNECT_REFUSED));
  }

  @Test
  public void tryLimitOthersAreDefaultMaxTry() {
    RetryPolicy p = policy();
    // A representative sampling of the "regular" failure types.
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.TIMEOUT));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.SERVER_IS_BUSY));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.SERVER_NOT_READY));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.OOM));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.UNKNOWN_ERROR));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.SERVER_HANDLE_FAILED));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.DATA_NOT_FOUND));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.SERIALIZE_RESPONSE_FAILED));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.PARTITION_READ_ONLY));
    assertEquals(MAX_TRY, p.tryLimit(ResponseType.CLOCK_NOTREADY));
  }

  @Test
  public void tryLimitNullFallsBackToDefault() {
    assertEquals(MAX_TRY, policy().tryLimit(null));
  }

  @Test
  public void tryLimitNetworkGuardsAgainstOverflow() {
    RetryPolicy huge = new RetryPolicy(0, 0, 0, Integer.MAX_VALUE - 5);
    // (Integer.MAX_VALUE - 5) * 2 would overflow to a negative number; the guard should
    // saturate to Integer.MAX_VALUE instead.
    assertEquals(Integer.MAX_VALUE, huge.tryLimit(ResponseType.NETWORK_ERROR));
  }

  // ---- back-off shape -------------------------------------------------------------------

  @Test
  public void networkBackoffStaysWithinWindow() {
    RetryPolicy p = policy();
    for (int i = 0; i < 1000; i++) {
      long bo = p.nextBackoffMs(ResponseType.NETWORK_ERROR, 1);
      assertTrue("network backoff must be >= 0", bo >= 0);
      assertTrue("network backoff must be < window: " + bo,
          bo < Math.max(50L, BACKOFF_WINDOW_MS));
    }
  }

  @Test
  public void serverNotReadyBackoffIsBoundedAndReasonable() {
    RetryPolicy p = policy();
    for (int i = 0; i < 1000; i++) {
      long bo = p.nextBackoffMs(ResponseType.SERVER_NOT_READY, 1);
      // 1000ms + random[0, 4000)  →  [1000, 5000)
      assertTrue("SERVER_NOT_READY backoff lower bound", bo >= 1000L);
      assertTrue("SERVER_NOT_READY backoff upper bound: " + bo, bo < 5000L);
    }
  }

  @Test
  public void busyAndOomBackoffStaysUnder30Seconds() {
    RetryPolicy p = policy();
    for (int try_ = 1; try_ <= 20; try_++) {
      for (int i = 0; i < 200; i++) {
        long busy = p.nextBackoffMs(ResponseType.SERVER_IS_BUSY, try_);
        long oom = p.nextBackoffMs(ResponseType.OOM, try_);
        // cap is 30s; the jittered upper bound is capped + capped/2 + 1 = 1.5 * cap + 1
        assertTrue("BUSY backoff capped: " + busy, busy <= 30_000L + 1L);
        assertTrue("OOM  backoff capped: " + oom, oom <= 30_000L + 1L);
        assertTrue(busy >= 0);
        assertTrue(oom >= 0);
      }
    }
  }

  @Test
  public void timeoutBackoffIsCappedAtRequestTimeout() {
    RetryPolicy p = policy();
    for (int try_ = 1; try_ <= 20; try_++) {
      for (int i = 0; i < 200; i++) {
        long bo = p.nextBackoffMs(ResponseType.TIMEOUT, try_);
        assertTrue("TIMEOUT backoff capped at requestTimeOut: " + bo,
            bo <= REQ_TIMEOUT_MS + 1L);
        assertTrue(bo >= 0);
      }
    }
  }

  @Test
  public void nullTypeFallsBackToLegacyUniform() {
    RetryPolicy p = policy();
    for (int i = 0; i < 1000; i++) {
      long bo = p.nextBackoffMs(null, 1);
      assertTrue("legacy fallback within request timeout", bo >= 0 && bo < REQ_TIMEOUT_MS);
    }
  }

  /**
   * Exponential back-off is unbounded up to its cap but the cap is finite — verify the
   * advertised cap (30s for BUSY/OOM, requestTimeOut for TIMEOUT) actually holds even for
   * very large {@code tryCounter}.
   */
  @Test
  public void hugeTryCounterDoesNotOverflowOrEscapeCap() {
    RetryPolicy p = policy();
    for (int tries : new int[] {100, 1000, Integer.MAX_VALUE}) {
      long busy = p.nextBackoffMs(ResponseType.SERVER_IS_BUSY, tries);
      long timeout = p.nextBackoffMs(ResponseType.TIMEOUT, tries);
      assertTrue("BUSY: huge tryCounter still capped: " + busy, busy <= 30_000L + 1L);
      assertTrue("TIMEOUT: huge tryCounter still capped: " + timeout,
          timeout <= REQ_TIMEOUT_MS + 1L);
    }
  }

  // ---- nextRetryTs ----------------------------------------------------------------------

  @Test
  public void nextRetryTsIsNowPlusBackoff() {
    RetryPolicy p = policy();
    long now = 100_000L;
    long ts = p.nextRetryTs(now, ResponseType.NETWORK_ERROR, 1);
    assertTrue("nextRetryTs must lie at or after now", ts >= now);
    assertTrue("nextRetryTs must lie within the network window of now",
        ts < now + Math.max(50L, BACKOFF_WINDOW_MS));
  }
}
