package com.tencent.angel.spark.ml.tree.tree.param;

import java.io.Serializable;

public abstract class TreeParam implements Serializable {
    public int numFeature;  // number of features
    public int maxDepth;  // maximum depth
    public int maxNodeNum;  // maximum node num
    public int numSplit;  // number of candidate splits
    public int numWorker;  // number of workers
    //public float insSampleRatio;  // subsample ratio for instances
    public float featSampleRatio;  // subsample ratio for features

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("|numFeature = %d\n", numFeature));
        sb.append(String.format("|maxDepth = %d\n", maxDepth));
        sb.append(String.format("|maxNodeNum = %d\n", maxNodeNum));
        sb.append(String.format("|numSplit = %d\n", numSplit));
        sb.append(String.format("|numWorker = %d\n", numWorker));
        sb.append(String.format("|featSampleRatio = %f\n", featSampleRatio));
        return sb.toString();
    }
}
