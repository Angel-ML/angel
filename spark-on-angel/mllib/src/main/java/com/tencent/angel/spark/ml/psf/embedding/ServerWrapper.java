package com.tencent.angel.spark.ml.psf.embedding;


/**
 * This class wraps some data structure that required to be maintained at server.
 */
public class ServerWrapper {

  // @sentences: this variable contains the sentences for each RDD partition
  public static int[][][] sentences;
  // @maxIndex: this variable contains the max index of node/word
  private static volatile int maxIndex = -1;

  private static float[][] contexts;


  public static synchronized void initialize(int numPartitions) {
    if (sentences == null) {
      sentences = new int[numPartitions][][];
      contexts = new float[numPartitions][];
    }
  }

  public static void setMaxIndex(int maxIndex) {
    ServerWrapper.maxIndex = maxIndex;
  }

  public static int getMaxIndex() {
    return ServerWrapper.maxIndex;
  }

  public static int[][] getSentencesWithPartitionId(int partitionId) {
    return sentences[partitionId];
  }

  public static void setSentences(int partitionId, int[][] partitionSentences) {
    sentences[partitionId] = partitionSentences;
  }

  public static void setContext(int partitionId, float[] negatives) {
    ServerWrapper.contexts[partitionId] = negatives;
  }

  public static float[] getContext(int partitionId) {
    return contexts[partitionId];
  }

}
