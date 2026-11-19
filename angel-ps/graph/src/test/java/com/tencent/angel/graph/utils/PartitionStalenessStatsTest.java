package com.tencent.angel.graph.utils;

import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link PartitionStalenessStats}.
 *
 * Verifies that staleness statistics (sum, count, max) are correctly
 * accumulated across multiple recordStaleness calls, and that reset()
 * clears all state.
 */
public class PartitionStalenessStatsTest {

    @Test
    public void testEmptyStats() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        long[] result = stats.getStats();
        assertEquals(0L, result[0]);  // sum
        assertEquals(0L, result[1]);  // count
        assertEquals(0L, result[2]);  // max
        assertEquals(0.0, stats.getStalenessSum(), 0.001);
        assertEquals(0.0, stats.getStalenessMax(), 0.001);
    }

    @Test
    public void testSingleRecord() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(5);
        long[] result = stats.getStats();
        assertEquals(5L, result[0]);   // sum
        assertEquals(1L, result[1]);   // count
        assertEquals(5L, result[2]);   // max
    }

    @Test
    public void testMultipleRecords() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(3);
        stats.recordStaleness(7);
        stats.recordStaleness(1);
        stats.recordStaleness(10);
        long[] result = stats.getStats();
        assertEquals(21L, result[0]);   // sum = 3+7+1+10
        assertEquals(4L, result[1]);    // count
        assertEquals(10L, result[2]);   // max
    }

    @Test
    public void testNegativeStalenessIgnored() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(-1);  // should be ignored
        stats.recordStaleness(5);
        stats.recordStaleness(-3);  // should be ignored
        long[] result = stats.getStats();
        assertEquals(5L, result[0]);   // sum
        assertEquals(1L, result[1]);   // count
        assertEquals(5L, result[2]);   // max
    }

    @Test
    public void testZeroStalenessRecorded() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(0);
        long[] result = stats.getStats();
        assertEquals(0L, result[0]);   // sum
        assertEquals(1L, result[1]);   // count = 1 (zero is valid)
        assertEquals(0L, result[2]);   // max
    }

    @Test
    public void testReset() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(5);
        stats.recordStaleness(10);
        stats.reset();
        long[] result = stats.getStats();
        assertEquals(0L, result[0]);
        assertEquals(0L, result[1]);
        assertEquals(0L, result[2]);
    }

    @Test
    public void testResetThenRecordAgain() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(100);
        stats.reset();
        stats.recordStaleness(3);
        long[] result = stats.getStats();
        assertEquals(3L, result[0]);
        assertEquals(1L, result[1]);
        assertEquals(3L, result[2]);
    }

    @Test
    public void testConcurrentRecords() throws InterruptedException {
        final PartitionStalenessStats stats = new PartitionStalenessStats();
        int numThreads = 4;
        int recordsPerThread = 1000;
        Thread[] threads = new Thread[numThreads];
        for (int t = 0; t < numThreads; t++) {
            threads[t] = new Thread(() -> {
                for (int i = 0; i < recordsPerThread; i++) {
                    stats.recordStaleness(1);
                }
            });
            threads[t].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }
        long[] result = stats.getStats();
        assertEquals((long) numThreads * recordsPerThread, result[1]);  // count
        assertEquals((long) numThreads * recordsPerThread, result[0]);  // sum (all 1s)
        assertEquals(1L, result[2]);  // max
    }

    @Test
    public void testLargeStaleness() {
        PartitionStalenessStats stats = new PartitionStalenessStats();
        stats.recordStaleness(Integer.MAX_VALUE);
        long[] result = stats.getStats();
        assertEquals((long) Integer.MAX_VALUE, result[0]);
        assertEquals(1L, result[1]);
        assertEquals((long) Integer.MAX_VALUE, result[2]);
    }
}
