package com.tencent.angel.spark.ml.tree.sketch;

import java.io.Serializable;

public abstract class QuantileSketch implements Serializable {
    protected long n; // total number of data items appeared
    protected long estimateN; // estimated total number of data items there will be,
    // if not -1, sufficient space will be allocated at once

    protected float minValue;
    protected float maxValue;

    public QuantileSketch(long estimateN) {
        this.estimateN = estimateN > 0 ? estimateN : -1L;
    }

    public QuantileSketch() {
        this(-1L);
    }

    public abstract void reset();

    public abstract void update(float value);

    public abstract void merge(QuantileSketch other);

    public abstract float getQuantile(float fraction);

    public abstract float[] getQuantiles(float[] fractions);

    public abstract float[] getQuantiles(int evenPartition);

    public boolean isEmpty() {
        return n == 0;
    }

    public long getN() {
        return n;
    }

    public long getEstimateN() {
        return estimateN;
    }

    public float getMinValue() {
        return minValue;
    }

    public float getMaxValue() {
        return maxValue;
    }
}
