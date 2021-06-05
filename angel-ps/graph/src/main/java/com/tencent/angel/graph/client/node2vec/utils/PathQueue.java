package com.tencent.angel.graph.client.node2vec.utils;

import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantLock;


public class PathQueue {

  private static Int2ObjectOpenHashMap<Queue<WalkPath>> psPartId2Queue = new Int2ObjectOpenHashMap<>();
  private static Int2ObjectOpenHashMap<ReentrantLock> psPartId2Lock = new Int2ObjectOpenHashMap<>();
  private static Int2IntOpenHashMap progress = new Int2IntOpenHashMap();
  private static int numParts = -1;
  private static int threshold = -1;
  private static double keepProba = 0.0;
  private static boolean isTrunc = false;

  public synchronized static void init(int psPartId) {
    if (!psPartId2Queue.containsKey(psPartId) || psPartId2Lock.get(psPartId) == null) {
      psPartId2Queue.put(psPartId, new LinkedList<>());
    } else {
      psPartId2Queue.get(psPartId).clear();
    }

    if (!psPartId2Lock.containsKey(psPartId) || psPartId2Lock.get(psPartId) == null) {
      ReentrantLock lock = new ReentrantLock();
      psPartId2Lock.put(psPartId, lock);
    }

    progress.put(psPartId, 0);
  }

  public synchronized static void clear(int psPartId) {
    if (psPartId2Queue.containsKey(psPartId)) {
      psPartId2Queue.get(psPartId).clear();
    }

    if (progress.containsKey(psPartId)) {
      progress.put(psPartId, 0);
    }
  }

  public static void initPushBatch(int psPartId, List<WalkPath> paths) {
    ReentrantLock queueLock = psPartId2Lock.get(psPartId);
    queueLock.lock();
    try {
      Queue<WalkPath> queue = psPartId2Queue.get(psPartId);
      queue.addAll(paths);
    } finally {
      queueLock.unlock();
    }
  }

  public static void pushBatch(int psPartId, ServerLongAnyRow row, Long2LongOpenHashMap pathTail) {
    ReentrantLock queueLock = psPartId2Lock.get(psPartId);
    int completeCount = 0;

    queueLock.lock();
    try {
      Queue<WalkPath> queue = psPartId2Queue.get(psPartId);
      for (Map.Entry<Long, Long> entry : pathTail.entrySet()) {
        long key = entry.getKey();
        long tail = entry.getValue();

        WalkPath wPath = (WalkPath) row.get(key);
        wPath.add2Path(tail);

        if (wPath.isComplete()) {
          completeCount += 1;
        } else {
          queue.add(wPath);
        }
      }

      progress.put(psPartId, progress.get(psPartId) + completeCount);
    } finally {
      queueLock.unlock();
    }
  }

  public static Long2ObjectOpenHashMap<long[]> popBatchTail(int psPartId, int batchSize) {
    ReentrantLock queueLock = psPartId2Lock.get(psPartId);
    queueLock.lock();
    Long2ObjectOpenHashMap<long[]> result;
    try {
      Queue<WalkPath> queue = psPartId2Queue.get(psPartId);
      int count = 0;
      result = new Long2ObjectOpenHashMap<>(batchSize);
      while (!queue.isEmpty() && count < batchSize) {
        WalkPath wPath = queue.poll();
        result.put(wPath.getHead(), wPath.getTail2());
        count++;
      }
    } finally {
      queueLock.unlock();
    }

    return result;
  }

  public static void popBatchTail(int psPartId, int batchSize,
      Long2ObjectOpenHashMap<long[]> result) {
    ReentrantLock queueLock = psPartId2Lock.get(psPartId);
    queueLock.lock();
    try {
      Queue<WalkPath> queue = psPartId2Queue.get(psPartId);
      int count = 0;
      while (!queue.isEmpty() && count < batchSize) {
        WalkPath wPath = queue.poll();
        result.put(wPath.getHead(), wPath.getTail2());
        count++;
      }
    } finally {
      queueLock.unlock();
    }
  }

  public static Long2ObjectOpenHashMap<long[]> popBatchPath(int psPartId, int batchSize) {
    ReentrantLock queueLock = psPartId2Lock.get(psPartId);
    queueLock.lock();
    Long2ObjectOpenHashMap<long[]> result;
    try {
      Queue<WalkPath> queue = psPartId2Queue.get(psPartId);
      int count = 0;
      result = new Long2ObjectOpenHashMap<>(batchSize);
      while (!queue.isEmpty() && count < batchSize) {
        WalkPath wPath = queue.poll();
        result.put(wPath.getHead(), wPath.getPath());
        count++;
      }
    } finally {
      queueLock.unlock();
    }

    return result;
  }

  public static void popBatchPath(int psPartId, int batchSize,
      Long2ObjectOpenHashMap<long[]> result) {
    ReentrantLock queueLock = psPartId2Lock.get(psPartId);
    queueLock.lock();
    try {
      Queue<WalkPath> queue = psPartId2Queue.get(psPartId);
      int count = 0;
      while (!queue.isEmpty() && count < batchSize) {
        WalkPath wPath = queue.poll();
        result.put(wPath.getHead(), wPath.getPath());
        count++;
      }
    } finally {
      queueLock.unlock();
    }
  }

  public static int getProgress(int psPartId) {
    return progress.get(psPartId);
  }

  public static int getThreshold() {
    return threshold;
  }

  public synchronized static void setThreshold(int threshold) {
    PathQueue.threshold = threshold;
  }

  public static double getKeepProba() {
    return keepProba;
  }

  public synchronized static void setKeepProba(double keepProba) {
    PathQueue.keepProba = keepProba;
  }

  public static boolean isIsTrunc() {
    return isTrunc;
  }

  public synchronized static void setIsTrunc(boolean isTrunc) {
    PathQueue.isTrunc = isTrunc;
  }

  public static int getNumParts() {
    return numParts;
  }

  public synchronized static void setNumParts(int numParts) {
    PathQueue.numParts = numParts;
  }
}
