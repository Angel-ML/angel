package com.tencent.angel.spark.ml.tree.objective.metric;

import com.google.common.base.Preconditions;
import com.tencent.angel.spark.ml.tree.exception.GBDTException;
import com.tencent.angel.spark.ml.tree.util.Maths;
import javax.inject.Singleton;

@Singleton
public class CrossEntropyMetric implements EvalMetric {
    private static CrossEntropyMetric instance;

    private CrossEntropyMetric() {}

    @Override
    public Kind getKind() {
        return Kind.CROSS_ENTROPY;
    }

    @Override
    public double eval(float[] preds, float[] labels) {
        Preconditions.checkArgument(preds.length != labels.length
                        && preds.length % labels.length == 0,
                "CrossEntropyMetric should be used for multi-label classification");
        double loss = 0.0;
        int numLabel = preds.length / labels.length;
        float[] pred = new float[numLabel];
        for (int i = 0; i < labels.length; i++) {
            System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
            loss += evalOne(pred, labels[i]);
        }
        return loss / labels.length;
    }

    @Override
    public double evalOne(float pred, float label) {
        throw new GBDTException("CrossEntropyMetric should be used for multi-label classification");
    }

    @Override
    public double evalOne(float[] pred, float label) {
        double sum = 0.0;
        for (float p : pred) {
            sum += Math.exp(p);
        }
        double p = Math.exp(pred[(int) label]) / sum;
        return -Maths.fastLog(Math.max(p, Maths.EPSILON));
    }

    public static CrossEntropyMetric getInstance() {
        if (instance == null)
            instance = new CrossEntropyMetric();
        return instance;
    }
}
