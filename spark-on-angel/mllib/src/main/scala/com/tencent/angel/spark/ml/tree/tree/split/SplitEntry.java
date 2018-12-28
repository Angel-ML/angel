package com.tencent.angel.spark.ml.tree.tree.split;

import java.io.Serializable;
import javax.validation.constraints.NotNull;
import org.apache.spark.ml.linalg.Vector;

public abstract class SplitEntry implements Serializable {
    protected int fid;
    protected float gain;

    public SplitEntry() {
        this(-1, 0.0f);
    }

    public SplitEntry(int fid, float gain) {
        this.fid = fid;
        this.gain = gain;
    }

    public boolean isEmpty() {
        return fid == -1;
    }

    public abstract int flowTo(float x);

    public abstract int flowTo(Vector x);

    public abstract int defaultTo();

    public boolean needReplace(float newGain) {
        return gain < newGain;
    }

    public boolean needReplace(@NotNull SplitEntry e) {
        return !compareTo(this, e);
    }

    /**
     * Comparison two split entries, returns true if e1 is better than e2
     *
     * @param e1
     * @param e2
     * @return
     */
    private static boolean compareTo(SplitEntry e1, SplitEntry e2) {
        if (e1.gain < e2.gain) {
            return false;
        } else if (e1.gain > e2.gain) {
            return true;
        } else {
            SplitType t1 = e1.splitType(), t2 = e2.splitType();
            if (t1 != t2) {
                // in order to reduce model size, we give priority to split point
                return t1.compareTo(t2) <= 0;
            } else if (t1 == SplitType.SPLIT_POINT) {
                // comparison between two split points, we give priority to lower feature index
                return e1.fid <= e2.fid;
            } else {
                // TODO: comparison between two split sets
                return true;
            }
        }
    }

    public int getFid() {
        return fid;
    }

    public void setFid(int fid) {
        this.fid = fid;
    }

    public float getGain() {
        return gain;
    }

    public void setGain(float gain) {
        this.gain = gain;
    }

    public abstract SplitType splitType();

    @Override
    public abstract String toString();
}
