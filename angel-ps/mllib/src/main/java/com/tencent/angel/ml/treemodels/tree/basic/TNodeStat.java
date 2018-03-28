package com.tencent.angel.ml.treemodels.tree.basic;

public abstract class TNodeStat {
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
