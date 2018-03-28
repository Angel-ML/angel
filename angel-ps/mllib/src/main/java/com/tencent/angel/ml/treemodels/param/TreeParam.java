package com.tencent.angel.ml.treemodels.param;

public abstract class TreeParam {
    public int numFeature;  // number of features
    public int maxDepth;  // maximum depth
    public int maxNodeNum;  // maximum node num
    public int numSplit;  // number of candidate splits
    public int numWorker;  // number of workers
    public float insSampleRatio;  // subsample ratio for instances
    public float featSampleRatio;  // subsample ratio for features
}
