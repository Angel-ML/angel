package com.tencent.angel.spark.ml.tree.tree.basic;

import java.io.Serializable;

public abstract class TNodeStat implements Serializable {
    protected float gain;  // gain of current node
    protected float nodeWeight;  // weight of current node

    public float getGain() {
        return gain;
    }

    public float getNodeWeight() {
        return nodeWeight;
    }

    public void setGain(float gain) {
        this.gain = gain;
    }

    public void setNodeWeight(float nodeWeight) {
        this.nodeWeight = nodeWeight;
    }
}
