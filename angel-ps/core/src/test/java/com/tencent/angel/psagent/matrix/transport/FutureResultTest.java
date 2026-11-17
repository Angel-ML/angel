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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.tencent.angel.exception.PSRPCException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Unit tests for the write-once guard added to {@link FutureResult}. Before the guard, a
 * SUCCESS arriving at the same moment as a TIMEOUT (or vice versa) could silently overwrite
 * the other side's outcome and the caller would observe the loser's value. The guard ensures
 * exactly one terminating call wins.
 */
public class FutureResultTest {

  @Test
  public void setThenSetIsIgnored() {
    FutureResult<String> fr = new FutureResult<>();
    fr.set("first");
    fr.set("second");
    try {
      assertEquals("first", fr.get(1, TimeUnit.SECONDS));
    } catch (Exception e) {
      fail("get() should not throw: " + e);
    }
    assertTrue(fr.isDone());
  }

  @Test
  public void setExceptionThenSetIsIgnored() {
    FutureResult<String> fr = new FutureResult<>();
    fr.setExecuteException(new ExecutionException(new PSRPCException("boom")));
    fr.set("ignored");
    try {
      fr.get(1, TimeUnit.SECONDS);
      fail("get() should have thrown");
    } catch (ExecutionException e) {
      assertNotNull(e.getCause());
    } catch (Exception other) {
      fail("unexpected: " + other);
    }
  }

  @Test
  public void setThenSetExceptionIsIgnored() throws Exception {
    FutureResult<String> fr = new FutureResult<>();
    fr.set("ok");
    fr.setExecuteException(new ExecutionException(new PSRPCException("late")));
    assertEquals("ok", fr.get(1, TimeUnit.SECONDS));
  }

  @Test
  public void getTimesOutWhenNeverSet() {
    FutureResult<String> fr = new FutureResult<>();
    try {
      fr.get(50, TimeUnit.MILLISECONDS);
      fail("get() should have timed out");
    } catch (TimeoutException expected) {
      // expected
    } catch (Exception other) {
      fail("unexpected: " + other);
    }
    assertFalse(fr.isDone());
  }

  /**
   * The race that motivated the CAS guard: N threads simultaneously try to terminate the
   * same future, some with set(value), some with setExecuteException(error). Whichever
   * arrives first must be the one the caller observes; all others must be silently dropped.
   */
  @Test
  public void concurrentTerminationProducesExactlyOneWinner() throws Exception {
    int threads = 32;
    int iterations = 500;
    AtomicInteger setWins = new AtomicInteger();
    AtomicInteger errWins = new AtomicInteger();

    for (int it = 0; it < iterations; it++) {
      FutureResult<Integer> fr = new FutureResult<>();
      ExecutorService pool = Executors.newFixedThreadPool(threads);
      CountDownLatch ready = new CountDownLatch(threads);
      CountDownLatch go = new CountDownLatch(1);
      for (int i = 0; i < threads; i++) {
        final int payload = i;
        final boolean useError = (i % 2 == 0);
        pool.submit(() -> {
          ready.countDown();
          try { go.await(); } catch (InterruptedException ignore) {}
          if (useError) {
            fr.setExecuteException(new ExecutionException(new PSRPCException("e-" + payload)));
          } else {
            fr.set(payload);
          }
        });
      }
      ready.await();
      go.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(2, TimeUnit.SECONDS));

      // The future must be terminated, and observable behaviour must be consistent: either
      // get() returns a value OR throws -- not "both alternately call by call".
      assertTrue("future not done after " + threads + " terminators", fr.isDone());
      boolean got;
      Object value = null;
      Throwable error = null;
      try {
        value = fr.get(100, TimeUnit.MILLISECONDS);
        got = true;
      } catch (ExecutionException e) {
        error = e;
        got = false;
      }
      if (got) {
        // If the winner was set(), value must be non-null (we only put positive ints in).
        assertNotNull(value);
        setWins.incrementAndGet();
      } else {
        assertNotNull(error);
        errWins.incrementAndGet();
      }
      // Repeat get() must yield the same outcome -- the winner is stable.
      try {
        Object v2 = fr.get(100, TimeUnit.MILLISECONDS);
        assertSame("get() must be stable across calls", value, v2);
        assertNull("first call won via set, repeat must not throw", error);
      } catch (ExecutionException e) {
        assertNotNull("first call won via exception, repeat must not return a value", error);
      }
    }
    // Sanity: both outcomes should have happened at least once across 500 iterations.
    assertTrue("never saw a set() winner", setWins.get() > 0);
    assertTrue("never saw a setExecuteException() winner", errWins.get() > 0);
  }

  @Test
  public void setExecuteErrorWrapsInPSRPCException() {
    FutureResult<Object> fr = new FutureResult<>();
    fr.setExecuteError("hello");
    try {
      fr.get(100, TimeUnit.MILLISECONDS);
      fail("expected ExecutionException");
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      // FutureResult.setExecuteError wraps in an ExecutionException(PSRPCException), so
      // get() throws ExecutionException whose cause is the wrapper ExecutionException whose
      // cause is the PSRPCException; we only insist on PSRPCException being reachable.
      while (c != null && !(c instanceof PSRPCException)) {
        c = c.getCause();
      }
      assertNotNull("PSRPCException must appear in cause chain", c);
      assertTrue(c.getMessage().contains("hello"));
    } catch (Exception other) {
      fail("unexpected: " + other);
    }
  }
}
