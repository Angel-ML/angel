package com.tencent.angel.graph.client.node2vec.utils;

import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class PathQueue {
  private static HashMap<Integer, List<LinkedBlockingQueue<WalkPath>>> psPartId2QueueList = new HashMap<>();
  private static ConcurrentHashMap<Integer, Integer> progress = new ConcurrentHashMap<>();

  public synchronized static void init(int psPartId, int numWorker) {
    if (!psPartId2QueueList.containsKey(psPartId)) {
      List<LinkedBlockingQueue<WalkPath>> queueList = new ArrayList<>(numWorker);
      for (int wid = 0; wid < numWorker; wid++) {
        queueList.add(new LinkedBlockingQueue<>());
      }
      psPartId2QueueList.put(psPartId, queueList);
    }

    progress.put(psPartId, 0);
  }

  public synchronized static List<LinkedBlockingQueue<WalkPath>> getQueueList(int psPartId) {
    return psPartId2QueueList.get(psPartId);
  }

  private synchronized static LinkedBlockingQueue<WalkPath> getQueue(int psPartId, int workerPartId) {
    return psPartId2QueueList.get(psPartId).get(workerPartId);
  }

  public static void pushPath(int psPartId, WalkPath path) {
    int workerPartId = path.getNextPartitionIdx();
    LinkedBlockingQueue<WalkPath> queue = psPartId2QueueList.get(psPartId).get(workerPartId);

    try {
      queue.put(path);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void pushPath(List<LinkedBlockingQueue<WalkPath>> queueList, WalkPath path) {
    int workerPartId = path.getNextPartitionIdx();
    LinkedBlockingQueue<WalkPath> queue = queueList.get(workerPartId);

    try {
      // System.out.println("PathQueue: put data to queue");
      queue.put(path);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void popBatch(int psPartId,
                              int workerPartId,
                              int batchSize,
                              Long2ObjectOpenHashMap<long[]> result) {
    LinkedBlockingQueue<WalkPath> queue = PathQueue.getQueue(psPartId, workerPartId);

    // System.out.println("queue.size: " + queue.size());
    int numRetry = 3;
    int count = 0;
    int retry = 1;

    while (retry < numRetry && count < batchSize) {
      try {
        WalkPath wPath = queue.poll(100L, TimeUnit.MILLISECONDS);

        // System.out.println("CurrPathIdx of " + wPath.getHead() + " is " + wPath.getCurrPathIdx());
        if (wPath != null) {
          result.put(wPath.getHead(), wPath.getTail2());
          count++;
        } else {
          break;
        }
      } catch (InterruptedException e) {
        retry++;
      }
    }
//      if (numRetry == retry) {
//        System.out.println("retried 3 time, got : " + result.size());
//      }

    // System.out.println("popBatch: " + result.size() +" | "+ count);
  }

  public static void popAll(int psPartId, int workerPartId, Long2ObjectOpenHashMap<long[]> result) {
    LinkedBlockingQueue<WalkPath> queue = PathQueue.getQueue(psPartId, workerPartId);

    try {
      while (!queue.isEmpty()) {
        WalkPath wPath = queue.take();
        result.put(wPath.getHead(), wPath.getTail2());
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void progressOne(int psPartId) {
    progress.put(psPartId, progress.get(psPartId) + 1);
  }

  public static int getProgress(int psPartId) {
    return progress.get(psPartId);
  }
}
