package com.tencent.angel.spark.ml.tree.tree.split;

import java.util.Arrays;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import com.tencent.angel.spark.ml.tree.util.Maths;

public class SplitSet extends SplitEntry {
    private float[] edges;
    private int firstFlow;
    private int defaultFlow;

    public SplitSet() {
        this(-1, 0.0f, null, -1, -1);
    }

    public SplitSet(int fid, float gain, float[] edges, int firstFlow, int defaultFlow) {
        super(fid, gain);
        this.edges = edges;
        this.firstFlow = firstFlow;
        this.defaultFlow = defaultFlow;
    }

    @Override
    public int flowTo(float x) {
        if (edges.length == 1) {
            return firstFlow;
        } else {
            int index = Maths.indexOf(edges, x);
            if (Maths.isEven(index))
                return firstFlow;
            else
                return 1 - firstFlow;
        }
    }

    @Override
    public int flowTo(Vector x) {
        if (x instanceof SparseVector) {
            SparseVector sv = (SparseVector) x;
            int t = Arrays.binarySearch(sv.indices(), fid);
            if (t >= 0)
                return flowTo((float) sv.values()[t]);
            else
                return defaultFlow;
        }
        return flowTo((float) x.apply(fid));
    }

    @Override
    public int defaultTo() {
        return defaultFlow;
    }

    @Override
    public SplitType splitType() {
        return SplitType.SPLIT_SET;
    }

    @Override
    public String toString() {
        return String.format("%s fid[%d] edges%s firstFlow[%d] defaultFlow[%d] gain[%f]",
                this.splitType(), fid, Arrays.toString(edges), firstFlow, defaultFlow, gain);
    }
}
