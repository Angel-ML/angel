/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.ps.server.data.response.ResponseType;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Per-ResponseType retry policy: how long to back off before the next attempt, and how many
 * attempts to allow in total before giving up. Extracted from {@code MatrixTransportClient}
 * so the policy is testable in isolation (the original implementation was buried inside a
 * private method of a non-static inner class, which couldn't be exercised by a UT without
 * standing up the full PSAgent).
 *
 * <p>Two parameters are needed at construction time:</p>
 * <ul>
 *   <li>{@code retryIntervalMs} — base unit for exponential back-off of TIMEOUT / BUSY / OOM
 *       category failures (corresponds to {@code angel.matrixtransfer.retry.interval.ms}).</li>
 *   <li>{@code retryBackoffWindowMs} — upper bound of the uniform random window used for
 *       NETWORK_ERROR / CONNECT_REFUSED (corresponds to
 *       {@code angel.matrixtransfer.retry.backoff.window.ms}).</li>
 *   <li>{@code requestTimeOutMs} — used as the cap of the TIMEOUT exponential back-off, and
 *       as the unbounded fallback for any unclassified failure type.</li>
 *   <li>{@code maxTryNum} — the default per-request try limit; some failure types override
 *       to a smaller (FATAL) or larger (NETWORK) bound based on this value.</li>
 * </ul>
 *
 * <p>All methods are deterministic given a fixed clock and {@link ThreadLocalRandom}; the
 * randomness is intentional jitter to avoid synchronised retry storms.</p>
 */
public final class RetryPolicy {

  private final int retryIntervalMs;
  private final int retryBackoffWindowMs;
  private final int requestTimeOutMs;
  private final int maxTryNum;

  public RetryPolicy(int retryIntervalMs, int retryBackoffWindowMs, int requestTimeOutMs,
      int maxTryNum) {
    this.retryIntervalMs = retryIntervalMs;
    this.retryBackoffWindowMs = retryBackoffWindowMs;
    this.requestTimeOutMs = requestTimeOutMs;
    this.maxTryNum = maxTryNum;
  }

  /**
   * @return the wall-clock timestamp at which a failure of {@code failedType} should next be
   *     retried, given the request has been tried {@code tryCounter} times so far. Uses
   *     {@link ThreadLocalRandom} for jitter.
   */
  public long nextRetryTs(long nowTs, ResponseType failedType, int tryCounter) {
    return nowTs + nextBackoffMs(failedType, tryCounter);
  }

  /**
   * Pure function variant of {@link #nextRetryTs} that returns the back-off delta in ms
   * instead of an absolute timestamp. Easier to test.
   */
  public long nextBackoffMs(ResponseType failedType, int tryCounter) {
    ThreadLocalRandom r = ThreadLocalRandom.current();
    // tryCounter starts at 1 on the first failure; clamp the exponent so we don't overflow.
    int exp = Math.min(Math.max(tryCounter - 1, 0), 10);
    if (failedType == null) {
      return (long) (requestTimeOutMs * r.nextDouble());
    }
    switch (failedType) {
      case NETWORK_ERROR:
      case CONNECT_REFUSED:
        // Uniform [0, window). The dead channel is invalidated synchronously on the same
        // call so a tight retry already lands on a fresh channel.
        return r.nextLong(Math.max(50L, (long) retryBackoffWindowMs));
      case SERVER_NOT_READY:
        // PS is presumably restarting — wait briefly but not minutes.
        return 1000L + r.nextLong(4000L);
      case SERVER_IS_BUSY:
      case OOM: {
        long base = Math.max(retryIntervalMs, 1000L);
        long capped = Math.min(30_000L, base * (1L << exp));
        return capped / 2 + r.nextLong(capped / 2 + 1);
      }
      case TIMEOUT: {
        long base = Math.max(retryIntervalMs, 1000L);
        long capped = Math.min((long) requestTimeOutMs, base * (1L << exp));
        return capped / 2 + r.nextLong(capped / 2 + 1);
      }
      default:
        return (long) (requestTimeOutMs * r.nextDouble());
    }
  }

  /**
   * @return the maximum number of tries allowed for a request whose last failure was
   *     {@code failedType}. FATAL → 1, transient network → 2 × maxTryNum (with overflow
   *     guard), everything else → maxTryNum (legacy default).
   */
  public int tryLimit(ResponseType failedType) {
    if (failedType == null) {
      return maxTryNum;
    }
    switch (failedType) {
      case SERVER_HANDLE_FATAL:
      case UNSUPPORT_REQUEST:
      case PARSE_HEADER_FAILED:
        return 1;
      case NETWORK_ERROR:
      case CONNECT_REFUSED:
        // Guard against integer overflow if an operator sets maxTryNum near Integer.MAX_VALUE.
        return (maxTryNum > Integer.MAX_VALUE / 2) ? Integer.MAX_VALUE : maxTryNum * 2;
      default:
        return maxTryNum;
    }
  }

  public int getRetryIntervalMs() { return retryIntervalMs; }

  public int getRetryBackoffWindowMs() { return retryBackoffWindowMs; }

  public int getRequestTimeOutMs() { return requestTimeOutMs; }

  public int getMaxTryNum() { return maxTryNum; }
}
