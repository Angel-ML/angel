package com.tencent.angel.graph.utils;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-PS-partition staleness statistics, maintained on the PS side.
 *
 * <p>Staleness definition: for each embedding update, τ = currentVersion -
 * pullVersion, where:
 * <ul>
 *   <li>currentVersion is the per-(node,field) version counter on PS,
 *       incremented on every update to that (node,field)</li>
 *   <li>pullVersion is the version the worker captured when it pulled the
 *       embedding (returned by the get PSF)</li>
 * </ul>
 *
 * <p>This captures "how many updates landed on this (node,field) between
 * the batch's pull and push" — the exact distributed staleness.
 *
 * <p>The stats also track how many gradients were dropped or scaled by
 * the staleness control policy (drop / scale).
 *
 * <p>The distinct (node, field) count is computed on-demand by traversing
 * the PS row at epoch end, not accumulated per-update.
 *
 * <p>The stats are accumulated across all updates in an epoch. Workers
 * fetch the summary at epoch end via a get PSF, then reset for the next
 * epoch.
 *
 * <p>Thread-safety: counters use AtomicLong.
 */
public class PartitionStalenessStats {

    private final AtomicLong stalenessSum = new AtomicLong(0);
    private final AtomicLong stalenessCount = new AtomicLong(0);
    private final AtomicLong stalenessMax = new AtomicLong(0);

    // Staleness control counters: how many grads were dropped / scaled.
    private final AtomicLong droppedCount = new AtomicLong(0);
    private final AtomicLong scaledCount = new AtomicLong(0);

    /**
     * Record staleness for a single (node, field) update.
     *
     * @param staleness τ = currentVersion - pullVersion
     */
    public void recordStaleness(int staleness) {
        if (staleness < 0) return;  // guard against anomalies
        stalenessSum.addAndGet(staleness);
        stalenessCount.incrementAndGet();
        long prevMax = stalenessMax.get();
        while (staleness > prevMax) {
            if (stalenessMax.compareAndSet(prevMax, staleness)) break;
            prevMax = stalenessMax.get();
        }
    }

    /** Record that a gradient was dropped due to staleness policy. */
    public void recordDropped() {
        droppedCount.incrementAndGet();
    }

    /** Record that a gradient was scaled down due to staleness policy. */
    public void recordScaled() {
        scaledCount.incrementAndGet();
    }

    /** Aggregate stats as a long array: [stalenessSum, stalenessCount, stalenessMax, droppedCount, scaledCount]. */
    public long[] getStats() {
        return new long[] {
            stalenessSum.get(),
            stalenessCount.get(),
            stalenessMax.get(),
            droppedCount.get(),
            scaledCount.get()
        };
    }

    /** Reset all stats for a new epoch. */
    public void reset() {
        stalenessSum.set(0);
        stalenessCount.set(0);
        stalenessMax.set(0);
        droppedCount.set(0);
        scaledCount.set(0);
    }

    public long getStalenessSum() { return stalenessSum.get(); }
    public long getStalenessCount() { return stalenessCount.get(); }
    public long getStalenessMax() { return stalenessMax.get(); }
    public long getDroppedCount() { return droppedCount.get(); }
    public long getScaledCount() { return scaledCount.get(); }
}
