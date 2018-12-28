package com.tencent.angel.spark.ml.tree.objective.metric;

import com.tencent.angel.spark.ml.tree.util.Maths;
import javax.inject.Singleton;

@Singleton
public class ErrorMetric implements EvalMetric {
    private static ErrorMetric instance;

    private ErrorMetric() {}

    @Override
    public Kind getKind() {
        return Kind.ERROR;
    }

    @Override
    public double eval(float[] preds, float[] labels) {
        double error = 0.0;
        if (preds.length == labels.length) {
            for (int i = 0; i < preds.length; i++) {
                error += evalOne(preds[i], labels[i]);
            }
        } else {
            int numLabel = preds.length / labels.length;
            float[] pred = new float[numLabel];
            for (int i = 0; i < labels.length; i++) {
                System.arraycopy(preds, i * numLabel, pred, 0, numLabel);
                error += evalOne(pred, labels[i]);
            }
        }
        return (float) (error / labels.length);
    }

    @Override
    public double evalOne(float pred, float label) {
        return pred >= 0.0f ? 1.0 - label : label;
    }

    @Override
    public double evalOne(float[] pred, float label) {
        return Maths.argmax(pred) != ((int) label) ? 1 : 0;
    }

    public static ErrorMetric getInstance() {
        if (instance == null)
            instance = new ErrorMetric();
        return instance;
    }
}
