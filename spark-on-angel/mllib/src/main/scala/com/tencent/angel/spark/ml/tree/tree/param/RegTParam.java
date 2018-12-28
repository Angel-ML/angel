package com.tencent.angel.spark.ml.tree.tree.param;

public class RegTParam extends TreeParam {
    public float learningRate;  // step size of one tree
    public float minSplitGain;  // minimum loss gain required for a split

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(String.format("|learningRate = %f\n", learningRate));
        sb.append(String.format("|minSplitGain = %f\n", minSplitGain));
        return sb.toString();
    }
}
