package com.tencent.angel.ml.treemodels.param;

public class RegTParam extends TreeParam {
    public int numClass;  // number of classes
    public float learningRate;  // step size of one tree
    public float minSplitGain;  // minimum loss gain required for a split
}
