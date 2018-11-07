package com.tencent.angel.spark.ml.psf.embedding;


/**
 * This class wraps some data structure that required to be maintained at server.
 */
public class ServerWrapper {

  // @maxIndex: this variable contains the max index of node/word
  private static volatile int maxIndex = -1;

  private static volatile int maxLength = -1;

  private static int[] numInputs;
  private static int[] numOutputs;


  public static synchronized void initialize(int numPartitions, int maxIndex, int maxLength) {
    if (ServerWrapper.maxIndex == -1) {
      numInputs = new int[numPartitions];
      numOutputs = new int[numPartitions];
      ServerWrapper.maxIndex = maxIndex;
      ServerWrapper.maxLength = maxLength;
    }
  }

  public static int getMaxIndex() {
    return maxIndex;
  }

  public static int getMaxLength() {
    return maxLength;
  }

  public static void setNumInputs(int partitionId, int num) {
    numInputs[partitionId] = num;
  }

  public static int getNumInputs(int partitionId) {
    return numInputs[partitionId];
  }

  public static void setNumOutputs(int partitionId, int num) {
    numOutputs[partitionId] = num;
  }

  public static int getNumOutputs(int partitionId) {
    return numOutputs[partitionId];
  }

}
