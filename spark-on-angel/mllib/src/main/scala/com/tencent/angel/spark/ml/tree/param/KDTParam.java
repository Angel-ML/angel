package com.tencent.angel.spark.ml.tree.param;

public class KDTParam extends TreeParam {

  public int numThread;  // parallelism

  public float minSplitVariance;  // minimum Variance required for a split
  public int minNodeInstance;   // minimum node instance required for a split
  public int maxSplitNumPerFeat;  // maximum split num of per feature

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(super.toString());
    sb.append(String.format("|minNodeInstance = %d\n", minNodeInstance));
    sb.append(String.format("|minSplitVariance = %f\n", minSplitVariance));
    sb.append(String.format("|maxSplitNumPerFeat = %d\n", maxSplitNumPerFeat));
    return sb.toString();
  }
}
