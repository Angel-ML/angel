/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ps.server.data.profiling;

import com.tencent.angel.common.AngelThreadFactory;
import com.tencent.angel.ps.server.data.TransportMethod;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Per-method RPC counters for the matrix-transfer server. Records per
 * {@link TransportMethod} the number of invocations, error count, accumulated handle
 * latency (ms), accumulated request/response bytes, and a max-handle-latency high water
 * mark. Designed for hot-path use: every counter is a lock-free AtomicLong[Array], the
 * record path is allocation-free, and there is no per-request {@code Map.compute} call.
 *
 * <p>A single background scheduler thread periodically prints a delta summary
 * (since previous tick) to {@link Log}, then snapshots the totals. The printer is
 * delta-based so a long-running PS shows current-load behaviour rather than a
 * lifetime average that buries recent regressions.
 *
 * <p>Indexed by {@code methodId} (1-based, see {@link TransportMethod}). Slot 0 is
 * unused. Unknown method ids fall into the {@code UNKNOWN} bucket via
 * {@link TransportMethod#valueOf(int)}.
 */
public class MatrixTransportServerMetrics {

  private static final Log LOG = LogFactory.getLog(MatrixTransportServerMetrics.class);

  /**
   * Process-wide singleton. The matrix-transfer server has at most one instance per
   * PS JVM, and the per-PSF handlers (which live in handler classes that don't have
   * a back-reference to {@link com.tencent.angel.ps.server.data.WorkerPool}) need a
   * way to reach the counters without us threading them through every Handler
   * constructor signature. The lifetime is the JVM lifetime.
   */
  private static final MatrixTransportServerMetrics INSTANCE = new MatrixTransportServerMetrics();

  public static MatrixTransportServerMetrics getInstance() {
    return INSTANCE;
  }

  /**
   * Methods are 1..16 (UNKNOWN). Allocate one extra slot so methodId can be used as
   * the index directly without subtracting 1; slot 0 is unused.
   */
  private static final int SLOT_COUNT = 32;

  /**
   * Lightweight latency histogram used for per-PSF percentile estimation.
   *
   * <p>To improve precision while keeping overhead low, each power-of-two interval
   * [2^k, 2^(k+1)) is subdivided into {@link #LATENCY_BUCKET_SUB_BUCKETS} equal-ratio
   * sub-buckets. Compared to a plain log2 histogram this gives finer p95/p99/p999
   * estimation with only a small fixed memory increase.
   */
  private static final int LATENCY_BUCKET_SUB_BUCKETS = 4;
  private static final int LATENCY_HIST_BUCKETS = 64 * LATENCY_BUCKET_SUB_BUCKETS;

  // All counters indexed by methodId. Each AtomicLongArray is independently atomic;
  // we deliberately do NOT try to keep them transactionally consistent with each
  // other across the record() call, because that would require a per-method lock and
  // the metrics are descriptive (a small read-skew is harmless).
  private final AtomicLongArray callCount = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray errorCount = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray totalHandleNanos = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray totalE2eNanos = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray totalRequestBytes = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray totalResponseBytes = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray maxHandleNanosTick = new AtomicLongArray(SLOT_COUNT);

  // ---- RPC queue (dispatch wait) counters ----
  // queueWaitNanos = nanoTime at handle() start - nanoTime when netty handed us the bytes,
  // i.e. how long the request sat in the RPCWorker / partition queue before a worker
  // picked it up. This is the metric that tells "the PS is slow because it is backlogged"
  // apart from "the PS is slow because the PSF itself is slow".
  private final AtomicLongArray totalQueueNanos = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray queuedCallCount = new AtomicLongArray(SLOT_COUNT);
  private final AtomicLongArray maxQueueNanosTick = new AtomicLongArray(SLOT_COUNT);
  /** Global queue-wait histogram (all methods), for percentile estimation. */
  private final AtomicLongArray queueNanosHist = new AtomicLongArray(LATENCY_HIST_BUCKETS);

  // Last printed snapshot, used to compute deltas.
  private final long[] lastCallCount = new long[SLOT_COUNT];
  private final long[] lastErrorCount = new long[SLOT_COUNT];
  private final long[] lastTotalHandleNanos = new long[SLOT_COUNT];
  private final long[] lastTotalE2eNanos = new long[SLOT_COUNT];
  private final long[] lastTotalRequestBytes = new long[SLOT_COUNT];
  private final long[] lastTotalResponseBytes = new long[SLOT_COUNT];
  private final long[] lastTotalQueueNanos = new long[SLOT_COUNT];
  private final long[] lastQueuedCallCount = new long[SLOT_COUNT];
  private final long[] lastQueueNanosHist = new long[LATENCY_HIST_BUCKETS];
  // Wall-clock millis at the moment of the previous tick, for delta-rate calculations.
  private long lastTickMs = System.currentTimeMillis();

  // Server lifetime aggregates (for one-shot summaries).
  private final AtomicLong totalRpcSinceStart = new AtomicLong(0);
  private final AtomicLong totalErrorsSinceStart = new AtomicLong(0);

  /**
   * Per-PSF-class buckets. Keyed by the fully-qualified class name returned by the
   * request (e.g. {@code com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple
   * .SampleWithTagSamples}). The map is grown only on first sight of a new PSF class
   * (one-time allocation per PSF type, of which there are typically &lt; 100 for an
   * entire workload), after which every record() is a single CHM.get() returning a
   * shared bucket with lock-free AtomicLong fields.
   *
   * <p>Set to {@code null} when {@code psfBreakdownEnabled} is false to avoid keeping
   * an empty CHM around — the disable-path is hot only at config init, never hot path.
   */
  private volatile ConcurrentHashMap<String, PsfBucket> psfBuckets;

  /** Cap on the number of PSF rows printed per tick (sorted by handleNanos). */
  private volatile int psfTopN = 20;

  private volatile ScheduledExecutorService scheduler;

  /**
   * Samples the CURRENT depth of the RPC dispatch queue(s). Registered by
   * {@code WorkerPool} at start-up; the printer thread calls it once per tick, so the
   * implementation may be as expensive as a few queue size() calls.
   */
  public interface QueueDepthSampler {

    /**
     * @return number of requests waiting in the dispatch queue(s) right now, or -1 if unknown
     */
    long queuedTasks();

    /**
     * @return number of worker threads currently executing a request, or -1 if unknown
     */
    int activeWorkers();

    /**
     * @return total worker thread count, or -1 if unknown
     */
    int poolSize();

    /**
     * @return dispatch mode description, e.g. "independent-worker-pool" / "netty-eventloop"
     */
    String mode();
  }

  private volatile QueueDepthSampler queueDepthSampler;

  public void setQueueDepthSampler(QueueDepthSampler sampler) {
    this.queueDepthSampler = sampler;
  }

  /**
   * Record one finished RPC.
   *
   * @param methodId        the {@link TransportMethod} id (1-based)
   * @param handleNanos     time spent inside {@code handle()} on the RPCWorker (or IO
   *                        thread for sync requests). Nanos to keep precision for sub-ms
   *                        small PSFs.
   * @param e2eNanos        time from netty receive to outbound flush start (i.e. server
   *                        wall-clock visible to the caller, excluding network).
   * @param requestBytes    deserialised request size in bytes
   * @param responseBytes   serialised response size in bytes (0 if not yet known)
   * @param success         false if {@code handler.handle} threw or response wasn't sent
   */
  public void record(int methodId, long handleNanos, long e2eNanos,
      long requestBytes, long responseBytes, boolean success) {
    if (methodId < 0 || methodId >= SLOT_COUNT) {
      methodId = TransportMethod.UNKNOWN.getMethodId();
    }
    callCount.incrementAndGet(methodId);
    if (!success) {
      errorCount.incrementAndGet(methodId);
      totalErrorsSinceStart.incrementAndGet();
    }
    totalHandleNanos.addAndGet(methodId, handleNanos);
    totalE2eNanos.addAndGet(methodId, e2eNanos);
    if (requestBytes > 0) {
      totalRequestBytes.addAndGet(methodId, requestBytes);
    }
    if (responseBytes > 0) {
      totalResponseBytes.addAndGet(methodId, responseBytes);
    }
    // Atomic max-update without locks: spin while observed is smaller.
    long prev;
    do {
      prev = maxHandleNanosTick.get(methodId);
      if (handleNanos <= prev) {
        break;
      }
    } while (!maxHandleNanosTick.compareAndSet(methodId, prev, handleNanos));
    totalRpcSinceStart.incrementAndGet();
  }

  /**
   * Record how long one request waited between "netty handed us the bytes" and "a worker
   * started handling it", i.e. the RPC dispatch-queue wait.
   *
   * <p>Hot path: 3 AtomicLong adds + 1 CAS, allocation-free. Called once per RPC from
   * {@code WorkerPool#handle}.
   *
   * @param methodId   the {@link TransportMethod} id (1-based)
   * @param queueNanos wait time in nanos; non-positive values are ignored
   * @param queued     true when the request really went through a worker queue; false for
   *                   the inline (sync, netty IO thread) path, which by definition never
   *                   queues and would otherwise dilute the average to 0
   */
  public void recordQueueWait(int methodId, long queueNanos, boolean queued) {
    if (!queued || queueNanos <= 0) {
      return;
    }
    if (methodId < 0 || methodId >= SLOT_COUNT) {
      methodId = TransportMethod.UNKNOWN.getMethodId();
    }
    queuedCallCount.incrementAndGet(methodId);
    totalQueueNanos.addAndGet(methodId, queueNanos);
    queueNanosHist.incrementAndGet(latencyBucketOfNanos(queueNanos));
    long prev;
    do {
      prev = maxQueueNanosTick.get(methodId);
      if (queueNanos <= prev) {
        break;
      }
    } while (!maxQueueNanosTick.compareAndSet(methodId, prev, queueNanos));
  }

  /**
   * Record one finished PSF call (GET_PSF or UPDATE_PSF), broken down by PSF class
   * name. Hot-path cost: 1 CHM.get() (returning a shared bucket already created on
   * first sight), then 4-5 AtomicLong adds + 1 CAS for the per-tick max watermark.
   * Allocation-free after first sight of each PSF class.
   *
   * <p>If {@link #enablePsfBreakdown} was never called (default behaviour) this is a
   * no-op via the volatile null check, costing one cache-hot read on the hot path.
   *
   * @param psfClassName  the FQCN of the PSF (e.g. obtained from
   *                      {@code Class.getName()} on the resolved {@code GetFunc} /
   *                      {@code UpdateFunc} class — that String is interned-by-Class
   *                      so equality and hashing on the CHM are stable across calls)
   */
  public void recordPsf(String psfClassName, long handleNanos,
      long requestBytes, long responseBytes, boolean success) {
    ConcurrentHashMap<String, PsfBucket> buckets = psfBuckets;
    if (buckets == null) {
      return;  // PSF breakdown disabled
    }
    if (psfClassName == null) {
      return;
    }
    // computeIfAbsent only allocates on first sight; after that this is a plain get().
    PsfBucket b = buckets.get(psfClassName);
    if (b == null) {
      // Race-free creation: lose the race == drop a single allocation, harmless.
      b = buckets.computeIfAbsent(psfClassName, k -> new PsfBucket());
    }
    b.callCount.incrementAndGet();
    if (!success) {
      b.errorCount.incrementAndGet();
    }
    b.totalHandleNanos.addAndGet(handleNanos);
    b.handleNanosHist.incrementAndGet(latencyBucketOfNanos(handleNanos));
    if (requestBytes > 0) b.totalRequestBytes.addAndGet(requestBytes);
    if (responseBytes > 0) b.totalResponseBytes.addAndGet(responseBytes);
    long prev;
    do {
      prev = b.maxHandleNanosTick.get();
      if (handleNanos <= prev) break;
    } while (!b.maxHandleNanosTick.compareAndSet(prev, handleNanos));
  }

  /**
   * Enable PSF-class-level breakdown. Idempotent. Disabled by default to keep the
   * hot path doing zero CHM lookups when the operator has not opted in.
   *
   * @param topN cap on number of PSF rows printed per tick (sorted by tick
   *             handleNanos descending). Use 0 / negative for "print all seen".
   */
  public void enablePsfBreakdown(int topN) {
    if (psfBuckets == null) {
      psfBuckets = new ConcurrentHashMap<>();
    }
    psfTopN = topN;
  }

  /**
   * Per-PSF-class lock-free counters. Mirrors the per-method AtomicLongArray slots but
   * fits in a single object so the CHM lookup yields the bucket directly.
   */
  private static final class PsfBucket {
    final AtomicLong callCount = new AtomicLong();
    final AtomicLong errorCount = new AtomicLong();
    final AtomicLong totalHandleNanos = new AtomicLong();
    final AtomicLong totalRequestBytes = new AtomicLong();
    final AtomicLong totalResponseBytes = new AtomicLong();
    final AtomicLong maxHandleNanosTick = new AtomicLong();
    final AtomicLongArray handleNanosHist = new AtomicLongArray(LATENCY_HIST_BUCKETS);

    // Snapshot at last tick boundary (only ever touched by the printer thread, so
    // plain longs are sufficient — no cross-thread visibility requirement here).
    long lastCallCount;
    long lastErrorCount;
    long lastTotalHandleNanos;
    long lastTotalRequestBytes;
    long lastTotalResponseBytes;
    final long[] lastHandleNanosHist = new long[LATENCY_HIST_BUCKETS];
  }

  /**
   * Start the periodic summary printer.
   *
   * @param intervalMs millis between prints; non-positive disables periodic logging
   *                   (call sites can still invoke {@link #logSummary} on demand).
   */
  public synchronized void start(long intervalMs) {
    if (scheduler != null) {
      return;
    }
    if (intervalMs <= 0) {
      LOG.info("MatrixTransportServer metrics: periodic logging disabled (interval=" + intervalMs + "ms)");
      return;
    }
    ScheduledThreadPoolExecutor exec = new ScheduledThreadPoolExecutor(1,
        new AngelThreadFactory("ps-rpc-metrics"));
    exec.setRemoveOnCancelPolicy(true);
    exec.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    exec.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    exec.scheduleAtFixedRate(this::logSummarySafe, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    scheduler = exec;
    LOG.info("MatrixTransportServer metrics: periodic logging enabled, interval=" + intervalMs + "ms");
  }

  public synchronized void stop() {
    if (scheduler != null) {
      scheduler.shutdownNow();
      scheduler = null;
    }
  }

  /**
   * Build a compact CPU / memory info string for inclusion in the PS-RPC-METRICS
   * log line. Returns {@code null} when metrics collection is not available (e.g.
   * non-HotSpot JVM). The first call to {@code getProcessCpuLoad()} may return -1
   * if the JVM hasn't collected enough samples yet.
   */
  private static String buildCpuMemInfo() {
    try {
      OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
      MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
      double loadAvg = osBean.getSystemLoadAverage();
      long heapUsedMB = memBean.getHeapMemoryUsage().getUsed() >> 20;
      long heapMaxMB  = memBean.getHeapMemoryUsage().getMax() >> 20;
      StringBuilder s = new StringBuilder(128);
      if (loadAvg >= 0) {
        s.append("loadAvg=").append(String.format("%.1f", loadAvg)).append(' ');
      }
      int availCores = Runtime.getRuntime().availableProcessors();
      s.append("availCores=").append(availCores).append(' ');
      if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
        com.sun.management.OperatingSystemMXBean sunBean =
            (com.sun.management.OperatingSystemMXBean) osBean;
        double pc = sunBean.getProcessCpuLoad();
        double sc = sunBean.getSystemCpuLoad();
        if (pc >= 0) {
          double procCoreUsage = pc * availCores;
          // top口径: 单核=100%，多核可>100%，例如使用2核显示200%。
          double procCpuPct = procCoreUsage * 100.0;
          s.append("procCpu=").append(String.format("%.2f", pc)).append(' ')
              .append("procCpuPct=").append(String.format("%.1f", procCpuPct)).append('%').append(' ')
              .append("procCores=").append(String.format("%.2f", procCoreUsage)).append(' ');
        }
        if (sc >= 0) {
          double sysCpuPct = sc * 100.0;
          s.append("sysCpu=").append(String.format("%.2f", sc)).append(' ')
              .append("sysCpuPct=").append(String.format("%.1f", sysCpuPct)).append('%').append(' ');
        }
      }
      s.append("heapUsed=").append(heapUsedMB).append("MB")
          .append(" heapMax=").append(heapMaxMB).append("MB");
      return s.toString();
    } catch (Exception ignored) {
      return null;
    }
  }

  /**
   * Compute and log the delta since the previous tick. Resets the per-tick max watermark.
   * Wrapped in a Throwable catch so a metrics bug never kills the scheduler.
   */
  private void logSummarySafe() {
    try {
      logSummary();
    } catch (Throwable t) {
      LOG.error("ps-rpc-metrics tick failed (metrics will resume on next interval)", t);
    }
  }

  public void logSummary() {
    long nowMs = System.currentTimeMillis();
    long elapsedMs = Math.max(1L, nowMs - lastTickMs);

    long deltaCallSum = 0;
    long deltaErrSum = 0;
    long deltaQueuedSum = 0;
    long deltaQueueNanosSum = 0;
    long maxQueueNanosAll = 0;
    List<String> rows = new ArrayList<>();

    for (int id = 1; id < SLOT_COUNT; id++) {
      long c = callCount.get(id);
      long delta = c - lastCallCount[id];
      if (delta <= 0 && c == 0) {
        continue;  // never seen this method, skip
      }
      long e = errorCount.get(id);
      long h = totalHandleNanos.get(id);
      long e2e = totalE2eNanos.get(id);
      long rb = totalRequestBytes.get(id);
      long sb = totalResponseBytes.get(id);
      long mx = maxHandleNanosTick.getAndSet(id, 0L);
      long q = totalQueueNanos.get(id);
      long qc = queuedCallCount.get(id);
      long qmx = maxQueueNanosTick.getAndSet(id, 0L);

      long deltaErr = e - lastErrorCount[id];
      long deltaH = h - lastTotalHandleNanos[id];
      long deltaE2e = e2e - lastTotalE2eNanos[id];
      long deltaRb = rb - lastTotalRequestBytes[id];
      long deltaSb = sb - lastTotalResponseBytes[id];
      long deltaQ = q - lastTotalQueueNanos[id];
      long deltaQc = qc - lastQueuedCallCount[id];

      lastCallCount[id] = c;
      lastErrorCount[id] = e;
      lastTotalHandleNanos[id] = h;
      lastTotalE2eNanos[id] = e2e;
      lastTotalRequestBytes[id] = rb;
      lastTotalResponseBytes[id] = sb;
      lastTotalQueueNanos[id] = q;
      lastQueuedCallCount[id] = qc;

      if (delta == 0) {
        continue;  // no activity this tick, but keep snapshot fresh and skip the row
      }

      deltaCallSum += delta;
      deltaErrSum += deltaErr;
      deltaQueuedSum += deltaQc;
      deltaQueueNanosSum += deltaQ;
      if (qmx > maxQueueNanosAll) {
        maxQueueNanosAll = qmx;
      }

      double qps = delta * 1000.0 / elapsedMs;
      double avgHandleMs = delta == 0 ? 0.0 : deltaH / 1_000_000.0 / delta;
      double avgE2eMs = delta == 0 ? 0.0 : deltaE2e / 1_000_000.0 / delta;
      double maxHandleMs = mx / 1_000_000.0;
      double avgQueueMs = deltaQc <= 0 ? 0.0 : deltaQ / 1_000_000.0 / deltaQc;
      double maxQueueMs = qmx / 1_000_000.0;
      double inMBps = deltaRb / 1024.0 / 1024.0 / (elapsedMs / 1000.0);
      double outMBps = deltaSb / 1024.0 / 1024.0 / (elapsedMs / 1000.0);

      TransportMethod m = TransportMethod.valueOf(id);
      String name = m == null ? ("METHOD_" + id) : m.name();
      rows.add(String.format(
          "%-15s calls=%d err=%d qps=%.1f avgHandle=%.2fms avgE2e=%.2fms maxHandle=%.2fms avgQueue=%.2fms maxQueue=%.2fms in=%.2fMB/s out=%.2fMB/s",
          name, delta, deltaErr, qps, avgHandleMs, avgE2eMs, maxHandleMs, avgQueueMs, maxQueueMs,
          inMBps, outMBps));
    }

    lastTickMs = nowMs;

    if (rows.isEmpty()) {
      // Still print the queue section: a PS can be idle in terms of FINISHED rpc while
      // its dispatch queue is badly backlogged (that is exactly the case worth seeing).
      LOG.info("[PS-RPC-METRICS] window=" + elapsedMs + "ms calls=0 (idle)\n  "
          + buildQueueSection(0, 0, 0, 0));
      return;
    }

    // Collect CPU / memory snapshot for this tick.
    String cpuMemInfo = buildCpuMemInfo();

    StringBuilder sb = new StringBuilder(256 + rows.size() * 128);
    sb.append("[PS-RPC-METRICS] window=").append(elapsedMs).append("ms")
        .append(" calls=").append(deltaCallSum)
        .append(" errors=").append(deltaErrSum)
        .append(" lifetimeRpc=").append(totalRpcSinceStart.get())
        .append(" lifetimeErr=").append(totalErrorsSinceStart.get());
    if (cpuMemInfo != null) {
      sb.append(' ').append(cpuMemInfo);
    }
    sb.append('\n').append("  ")
        .append(buildQueueSection(deltaCallSum, deltaQueuedSum, deltaQueueNanosSum,
            maxQueueNanosAll));
    for (String r : rows) {
      sb.append('\n').append("  ").append(r);
    }

    // PSF-class breakdown (only if enabled). Sorted by THIS-TICK handleNanos delta
    // descending so the slowest-this-tick PSFs appear first, then truncated to topN.
    ConcurrentHashMap<String, PsfBucket> buckets = psfBuckets;
    if (buckets != null && !buckets.isEmpty()) {
      List<PsfRow> psfRows = new ArrayList<>(buckets.size());
      for (Map.Entry<String, PsfBucket> e : buckets.entrySet()) {
        PsfBucket b = e.getValue();
        // Snapshot atomics (small read-skew across them is harmless for descriptive
        // metrics); subtract previous tick to get this-window delta.
        long c = b.callCount.get();
        long deltaCalls = c - b.lastCallCount;
        if (deltaCalls <= 0) {
          // Still update snapshot so the next tick's delta is accurate.
          b.lastCallCount = c;
          b.lastErrorCount = b.errorCount.get();
          b.lastTotalHandleNanos = b.totalHandleNanos.get();
          b.lastTotalRequestBytes = b.totalRequestBytes.get();
          b.lastTotalResponseBytes = b.totalResponseBytes.get();
          for (int i = 0; i < LATENCY_HIST_BUCKETS; i++) {
            b.lastHandleNanosHist[i] = b.handleNanosHist.get(i);
          }
          b.maxHandleNanosTick.set(0L);
          continue;
        }
        long er = b.errorCount.get();
        long h = b.totalHandleNanos.get();
        long rb = b.totalRequestBytes.get();
        long sb2 = b.totalResponseBytes.get();
        long mx = b.maxHandleNanosTick.getAndSet(0L);

        long deltaErr = er - b.lastErrorCount;
        long deltaH = h - b.lastTotalHandleNanos;
        long deltaRb = rb - b.lastTotalRequestBytes;
        long deltaSb = sb2 - b.lastTotalResponseBytes;

        b.lastCallCount = c;
        b.lastErrorCount = er;
        b.lastTotalHandleNanos = h;
        b.lastTotalRequestBytes = rb;
        b.lastTotalResponseBytes = sb2;

        long t50 = quantileTarget(deltaCalls, 0.50d);
        long t90 = quantileTarget(deltaCalls, 0.90d);
        long t95 = quantileTarget(deltaCalls, 0.95d);
        long t99 = quantileTarget(deltaCalls, 0.99d);
        long t999 = quantileTarget(deltaCalls, 0.999d);
        long seen = 0L;
        long p50Ns = 0L;
        long p90Ns = 0L;
        long p95Ns = 0L;
        long p99Ns = 0L;
        long p999Ns = 0L;
        for (int i = 0; i < LATENCY_HIST_BUCKETS; i++) {
          long cur = b.handleNanosHist.get(i);
          long delta = cur - b.lastHandleNanosHist[i];
          b.lastHandleNanosHist[i] = cur;
          if (delta <= 0) {
            continue;
          }
          seen += delta;
          long bucketUpperNs = latencyBucketUpperBoundNanos(i);
          if (p50Ns == 0L && seen >= t50) p50Ns = bucketUpperNs;
          if (p90Ns == 0L && seen >= t90) p90Ns = bucketUpperNs;
          if (p95Ns == 0L && seen >= t95) p95Ns = bucketUpperNs;
          if (p99Ns == 0L && seen >= t99) p99Ns = bucketUpperNs;
          if (p999Ns == 0L && seen >= t999) p999Ns = bucketUpperNs;
        }

        if (p50Ns == 0L) p50Ns = mx;
        if (p90Ns == 0L) p90Ns = mx;
        if (p95Ns == 0L) p95Ns = mx;
        if (p99Ns == 0L) p99Ns = mx;
        if (p999Ns == 0L) p999Ns = mx;

        psfRows.add(new PsfRow(e.getKey(), deltaCalls, deltaErr, deltaH, mx, deltaRb, deltaSb,
            p50Ns, p90Ns, p95Ns, p99Ns, p999Ns));
      }

      if (!psfRows.isEmpty()) {
        // Sort by total handle time this tick descending — surfaces the PSFs that hurt
        // the most regardless of call frequency (a single 5s call beats 1000 fast ones).
        Collections.sort(psfRows, new Comparator<PsfRow>() {
          @Override public int compare(PsfRow a, PsfRow b1) {
            return Long.compare(b1.totalHandleNanos, a.totalHandleNanos);
          }
        });
        int cap = psfTopN;
        int limit = (cap > 0 && cap < psfRows.size()) ? cap : psfRows.size();
        sb.append('\n').append("  -- per-PSF (top ").append(limit)
            .append(" of ").append(psfRows.size()).append(" by handleTime) --");
        for (int i = 0; i < limit; i++) {
          PsfRow r = psfRows.get(i);
          double qps = r.calls * 1000.0 / elapsedMs;
          double avgHandleMs = r.totalHandleNanos / 1_000_000.0 / r.calls;
          double maxHandleMs = r.maxHandleNanos / 1_000_000.0;
          double p50Ms = r.p50Nanos / 1_000_000.0;
          double p90Ms = r.p90Nanos / 1_000_000.0;
          double p95Ms = r.p95Nanos / 1_000_000.0;
          double p99Ms = r.p99Nanos / 1_000_000.0;
          double p999Ms = r.p999Nanos / 1_000_000.0;
          double totalReqMB = r.requestBytes / 1024.0 / 1024.0;
          double totalRespMB = r.responseBytes / 1024.0 / 1024.0;
          double avgReqKB = r.requestBytes / 1024.0 / r.calls;
          double avgRespKB = r.responseBytes / 1024.0 / r.calls;
          double inMBps = r.requestBytes / 1024.0 / 1024.0 / (elapsedMs / 1000.0);
          double outMBps = r.responseBytes / 1024.0 / 1024.0 / (elapsedMs / 1000.0);
          // Strip the package prefix to keep the line readable; the FQCN is logged
          // anyway by ParameterServer at startup so the simple name is enough here.
          String simpleName = stripPackage(r.psfClassName);
          sb.append('\n').append("    ").append(String.format(
              "%-50s calls=%d err=%d qps=%.1f avgHandle=%.2fms p50=%.2fms p90=%.2fms p95=%.2fms p99=%.2fms p999=%.2fms maxHandle=%.2fms req=%.2fMB resp=%.2fMB avgReq=%.2fKB avgResp=%.2fKB in=%.2fMB/s out=%.2fMB/s",
              simpleName, r.calls, r.errors, qps, avgHandleMs, p50Ms, p90Ms, p95Ms,
              p99Ms, p999Ms, maxHandleMs, totalReqMB, totalRespMB, avgReqKB, avgRespKB,
              inMBps, outMBps));
        }
      }
    }

    LOG.info(sb.toString());
  }

  /**
   * Build the "rpc queue" line: how long requests waited before a worker picked them up
   * during this window, plus the CURRENT backlog depth sampled from the dispatch queue.
   *
   * <p>Reading it:
   * <ul>
   *   <li>{@code avgQueue}/{@code p99} near 0 and {@code depth}=0 → no backlog; slow RPC
   *       means the handler itself is slow (see per-method {@code avgHandle}).</li>
   *   <li>{@code avgQueue} comparable to or larger than {@code avgHandle}, {@code depth}
   *       growing, {@code activeWorkers} == {@code poolSize} → the PS is worker-bound;
   *       raise {@code angel.matrixtransfer.server.worker.pool.size} or reduce the client
   *       concurrency / request size.</li>
   *   <li>{@code depth} large while {@code activeWorkers} &lt; {@code poolSize} → workers
   *       are blocked (lock contention / GC / IO), not simply oversubscribed.</li>
   * </ul>
   *
   * <p>Always consumes the histogram delta so percentiles stay per-window even on idle
   * ticks.
   */
  private String buildQueueSection(long deltaCalls, long deltaQueued, long deltaQueueNanos,
      long maxQueueNanos) {
    long t50 = quantileTarget(deltaQueued, 0.50d);
    long t90 = quantileTarget(deltaQueued, 0.90d);
    long t95 = quantileTarget(deltaQueued, 0.95d);
    long t99 = quantileTarget(deltaQueued, 0.99d);
    long t999 = quantileTarget(deltaQueued, 0.999d);
    long seen = 0L;
    long p50Ns = 0L;
    long p90Ns = 0L;
    long p95Ns = 0L;
    long p99Ns = 0L;
    long p999Ns = 0L;
    for (int i = 0; i < LATENCY_HIST_BUCKETS; i++) {
      long cur = queueNanosHist.get(i);
      long delta = cur - lastQueueNanosHist[i];
      lastQueueNanosHist[i] = cur;
      if (delta <= 0 || deltaQueued <= 0) {
        continue;
      }
      seen += delta;
      long bucketUpperNs = latencyBucketUpperBoundNanos(i);
      if (p50Ns == 0L && seen >= t50) p50Ns = bucketUpperNs;
      if (p90Ns == 0L && seen >= t90) p90Ns = bucketUpperNs;
      if (p95Ns == 0L && seen >= t95) p95Ns = bucketUpperNs;
      if (p99Ns == 0L && seen >= t99) p99Ns = bucketUpperNs;
      if (p999Ns == 0L && seen >= t999) p999Ns = bucketUpperNs;
    }
    if (p50Ns == 0L) p50Ns = maxQueueNanos;
    if (p90Ns == 0L) p90Ns = maxQueueNanos;
    if (p95Ns == 0L) p95Ns = maxQueueNanos;
    if (p99Ns == 0L) p99Ns = maxQueueNanos;
    if (p999Ns == 0L) p999Ns = maxQueueNanos;

    double avgQueueMs = deltaQueued <= 0 ? 0.0 : deltaQueueNanos / 1_000_000.0 / deltaQueued;
    double queuedRatio = deltaCalls <= 0 ? 0.0 : deltaQueued * 100.0 / deltaCalls;

    StringBuilder s = new StringBuilder(220);
    s.append("-- rpc queue -- ")
        .append("queued=").append(deltaQueued)
        .append('/').append(deltaCalls)
        .append(String.format(" (%.1f%%)", queuedRatio))
        .append(String.format(" avgQueue=%.2fms", avgQueueMs))
        .append(String.format(" p50=%.2fms", p50Ns / 1_000_000.0))
        .append(String.format(" p90=%.2fms", p90Ns / 1_000_000.0))
        .append(String.format(" p95=%.2fms", p95Ns / 1_000_000.0))
        .append(String.format(" p99=%.2fms", p99Ns / 1_000_000.0))
        .append(String.format(" p999=%.2fms", p999Ns / 1_000_000.0))
        .append(String.format(" maxQueue=%.2fms", maxQueueNanos / 1_000_000.0));

    QueueDepthSampler sampler = queueDepthSampler;
    if (sampler != null) {
      try {
        long depth = sampler.queuedTasks();
        int active = sampler.activeWorkers();
        int poolSize = sampler.poolSize();
        s.append(" depth=").append(depth < 0 ? "n/a" : String.valueOf(depth))
            .append(" activeWorkers=").append(active < 0 ? "n/a" : String.valueOf(active))
            .append('/').append(poolSize < 0 ? "n/a" : String.valueOf(poolSize))
            .append(" mode=").append(sampler.mode());
      } catch (Throwable t) {
        s.append(" depth=err(").append(t.getClass().getSimpleName()).append(')');
      }
    }
    return s.toString();
  }

  /**
   * Build a side-effect-free snapshot of the RPC dispatch backlog, safe to call from any
   * thread at any time (e.g. the PS heartbeat thread).
   *
   * <p>Unlike {@link #logSummary()} this method touches NO delta bookkeeping: it neither
   * resets the per-tick max watermarks nor advances the {@code last*} snapshots, so the
   * periodic printer keeps producing correct per-window numbers. Consequently the latency
   * figures here are lifetime averages, while the depth figures are instantaneous.
   *
   * @return a compact one-line description, never null
   */
  public String buildQueueSnapshot() {
    long queuedCalls = 0;
    long queueNanos = 0;
    for (int id = 1; id < SLOT_COUNT; id++) {
      queuedCalls += queuedCallCount.get(id);
      queueNanos += totalQueueNanos.get(id);
    }
    double avgQueueMs = queuedCalls <= 0 ? 0.0 : queueNanos / 1_000_000.0 / queuedCalls;

    StringBuilder s = new StringBuilder(200);
    QueueDepthSampler sampler = queueDepthSampler;
    if (sampler != null) {
      try {
        long depth = sampler.queuedTasks();
        int active = sampler.activeWorkers();
        int poolSize = sampler.poolSize();
        s.append("depth=").append(depth < 0 ? "n/a" : String.valueOf(depth))
            .append(" activeWorkers=").append(active < 0 ? "n/a" : String.valueOf(active))
            .append('/').append(poolSize < 0 ? "n/a" : String.valueOf(poolSize))
            .append(" mode=").append(sampler.mode()).append(' ');
      } catch (Throwable t) {
        s.append("depth=err(").append(t.getClass().getSimpleName()).append(") ");
      }
    } else {
      s.append("depth=n/a(sampler-not-registered) ");
    }
    s.append("lifetimeRpc=").append(totalRpcSinceStart.get())
        .append(" lifetimeQueued=").append(queuedCalls)
        .append(String.format(" lifetimeAvgQueue=%.2fms", avgQueueMs));
    return s.toString();
  }

  private static long quantileTarget(long n, double q) {    if (n <= 0) {
      return 1L;
    }
    return Math.max(1L, (long) Math.ceil(n * q));
  }

  private static int latencyBucketOfNanos(long nanos) {
    if (nanos <= 0) {
      return 0;
    }
    int exp = 63 - Long.numberOfLeadingZeros(nanos);
    if (exp < 0) {
      return 0;
    }
    if (exp >= 63) {
      return LATENCY_HIST_BUCKETS - 1;
    }

    long base = 1L << exp;
    long offset = nanos - base;
    int sub = (int) (((double) offset * LATENCY_BUCKET_SUB_BUCKETS) / (double) base);
    if (sub < 0) {
      sub = 0;
    } else if (sub >= LATENCY_BUCKET_SUB_BUCKETS) {
      sub = LATENCY_BUCKET_SUB_BUCKETS - 1;
    }

    int bucket = exp * LATENCY_BUCKET_SUB_BUCKETS + sub;
    if (bucket < 0) {
      return 0;
    }
    if (bucket >= LATENCY_HIST_BUCKETS) {
      return LATENCY_HIST_BUCKETS - 1;
    }
    return bucket;
  }

  private static long latencyBucketUpperBoundNanos(int bucket) {
    if (bucket <= 0) {
      return 1L;
    }
    if (bucket >= LATENCY_HIST_BUCKETS - 1) {
      return Long.MAX_VALUE;
    }
    int exp = bucket / LATENCY_BUCKET_SUB_BUCKETS;
    int sub = bucket % LATENCY_BUCKET_SUB_BUCKETS;
    if (exp >= 62) {
      return Long.MAX_VALUE;
    }
    double base = (double) (1L << exp);
    double ratio = 1.0d + (sub + 1.0d) / LATENCY_BUCKET_SUB_BUCKETS;
    double upper = base * ratio;
    if (upper >= Long.MAX_VALUE) {
      return Long.MAX_VALUE;
    }
    long bounded = (long) Math.ceil(upper);
    return bounded <= 0 ? Long.MAX_VALUE : bounded;
  }

  private static String stripPackage(String fqcn) {
    int idx = fqcn.lastIndexOf('.');
    return idx < 0 ? fqcn : fqcn.substring(idx + 1);
  }

  /** Transient row used only by the printer thread; never escapes logSummary. */
  private static final class PsfRow {
    final String psfClassName;
    final long calls;
    final long errors;
    final long totalHandleNanos;
    final long maxHandleNanos;
    final long requestBytes;
    final long responseBytes;
    final long p50Nanos;
    final long p90Nanos;
    final long p95Nanos;
    final long p99Nanos;
    final long p999Nanos;

    PsfRow(String psfClassName, long calls, long errors, long totalHandleNanos,
           long maxHandleNanos, long requestBytes, long responseBytes,
           long p50Nanos, long p90Nanos, long p95Nanos, long p99Nanos, long p999Nanos) {
      this.psfClassName = psfClassName;
      this.calls = calls;
      this.errors = errors;
      this.totalHandleNanos = totalHandleNanos;
      this.maxHandleNanos = maxHandleNanos;
      this.requestBytes = requestBytes;
      this.responseBytes = responseBytes;
      this.p50Nanos = p50Nanos;
      this.p90Nanos = p90Nanos;
      this.p95Nanos = p95Nanos;
      this.p99Nanos = p99Nanos;
      this.p999Nanos = p999Nanos;
    }
  }
}
