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

  private static int[] numInputs;
  private static int[] numOutputs;
  private static int concurrentLevel;


  public static synchronized void initialize(int numPartitions, int concurrentLevel) {
    if (sentences == null) {
      sentences = new int[numPartitions][][];
      contexts = new float[numPartitions][];
      numInputs = new int[numPartitions * concurrentLevel];
      numOutputs = new int[numPartitions * concurrentLevel];
      ServerWrapper.concurrentLevel = concurrentLevel;
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

  public static void setNumInputs(int partitionId, int num, int threadId) {
    numInputs[partitionId * concurrentLevel + threadId] = num;
  }

  public static int getNumInputs(int partitionId, int threadId) {
    return numInputs[partitionId * concurrentLevel + threadId];
  }

  public static void setNumOutputs(int partitionId, int num, int threadId) {
    numOutputs[partitionId * concurrentLevel + threadId] = num;
  }

  public static int getNumOutputs(int partitionId, int threadId) {
    return numOutputs[partitionId * concurrentLevel + threadId];
  }

}
