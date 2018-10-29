package com.tencent.angel.spark.ml.psf.embedding;

public class ServerSentences {

  public static int[][][] batches;

  public static synchronized void initialize(int numPartitions) {
    if (batches == null)
      batches = new int[numPartitions][][];
  }

  public static int[][] getSentences(int partitionId) {
    return batches[partitionId];
  }
}
