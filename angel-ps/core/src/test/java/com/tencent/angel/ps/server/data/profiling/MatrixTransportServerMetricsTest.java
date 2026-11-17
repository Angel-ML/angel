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
package com.tencent.angel.ps.server.data.profiling;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLongArray;
import org.junit.Test;

/**
 * Unit tests for {@link MatrixTransportServerMetrics}:
 * 1) verify finer sub-bucket histogram behaviour;
 * 2) verify histogram data is updated correctly;
 * 3) add a lightweight performance guard for hot-path recordPsf.
 */
public class MatrixTransportServerMetricsTest {

  private static final String PSF = "com.tencent.angel.test.psf.MockPsf";

  @Test
  public void subBucketsSplitWithinSamePowerOfTwoRange() throws Exception {
    Method bucketOf = MatrixTransportServerMetrics.class
        .getDeclaredMethod("latencyBucketOfNanos", long.class);
    bucketOf.setAccessible(true);

    long low = 1025L;   // [2^10, 2^11)
    long mid = 1300L;   // [2^10, 2^11)
    long high = 1900L;  // [2^10, 2^11)

    int bLow = (int) bucketOf.invoke(null, low);
    int bMid = (int) bucketOf.invoke(null, mid);
    int bHigh = (int) bucketOf.invoke(null, high);

    assertTrue("same 2^k interval should now be split into finer sub-buckets", bLow < bMid);
    assertTrue("same 2^k interval should now be split into finer sub-buckets", bMid < bHigh);
  }

  @Test
  public void recordPsfUpdatesHistogramBucket() throws Exception {
    MatrixTransportServerMetrics metrics = new MatrixTransportServerMetrics();
    metrics.enablePsfBreakdown(10);

    Method bucketOf = MatrixTransportServerMetrics.class
        .getDeclaredMethod("latencyBucketOfNanos", long.class);
    bucketOf.setAccessible(true);

    long latencyNs = 1500L;
    int bucket = (int) bucketOf.invoke(null, latencyNs);

    metrics.recordPsf(PSF, latencyNs, 128L, 64L, true);

    Field bucketsField = MatrixTransportServerMetrics.class.getDeclaredField("psfBuckets");
    bucketsField.setAccessible(true);
    Map<String, ?> buckets = (Map<String, ?>) bucketsField.get(metrics);
    Object psfBucket = buckets.get(PSF);

    Field histField = psfBucket.getClass().getDeclaredField("handleNanosHist");
    histField.setAccessible(true);
    AtomicLongArray hist = (AtomicLongArray) histField.get(psfBucket);

    assertTrue("expected histogram bucket to be incremented", hist.get(bucket) == 1L);
  }

  @Test(timeout = 12000L)
  public void recordPsfHotPathPerformanceGuard() {
    MatrixTransportServerMetrics metrics = new MatrixTransportServerMetrics();
    metrics.enablePsfBreakdown(10);

    final int warmup = 20_000;
    final int iters = 300_000;

    for (int i = 0; i < warmup; i++) {
      metrics.recordPsf(PSF, 2_000L + (i & 1023), 256L, 0L, true);
    }

    long start = System.nanoTime();
    for (int i = 0; i < iters; i++) {
      metrics.recordPsf(PSF, 2_000L + (i & 1023), 256L, 0L, true);
    }
    long elapsedNs = System.nanoTime() - start;

    double avgNs = elapsedNs * 1.0d / iters;
    assertTrue("recordPsf should stay low-overhead on hot path, avgNs=" + avgNs,
        avgNs < 20_000.0d);
  }
}
