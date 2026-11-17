package com.tencent.angel.spark.ml.ann_core;

import java.io.Serializable;

public class NeighborDistance implements Serializable {
    private long neighbor;
    private float distance;

    public NeighborDistance(long neighbor, float distance) {
        this.neighbor = neighbor;
        this.distance = distance;
    }

    public NeighborDistance() {
        this.distance = 1000000.0f;
        this.neighbor = -1;
    }

    public long getNeighbor() {
        return this.neighbor;
    }

    public float getDistance() {
        return this.distance;
    }
}
