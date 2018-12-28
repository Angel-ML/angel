package com.tencent.angel.spark.ml.tree.objective.metric;

import com.google.common.base.Preconditions;
import com.tencent.angel.spark.ml.tree.exception.GBDTException;
import com.tencent.angel.spark.ml.tree.util.Maths;
import javax.inject.Singleton;

@Singleton
public class LogLossMetric implements EvalMetric {
    private static LogLossMetric instance;

    private LogLossMetric() {}

    @Override
    public Kind getKind() {
        return Kind.LOG_LOSS;
    }

    @Override
    public double eval(float[] preds, float[] labels) {
        Preconditions.checkArgument(preds.length == labels.length,
                "LogLossMetric should be used for binary-label classification");
        double loss = 0.0;
        for (int i = 0; i < preds.length; i++) {
            loss += evalOne(preds[i], labels[i]);
        }
        return loss / labels.length;
    }

    @Override
    public double evalOne(float pred, float label) {
        float prob = Maths.fastSigmoid(pred);
        return -(label * Maths.fastLog(prob) + (1 - label) * Maths.fastLog(1 - prob));
    }

    @Override
    public double evalOne(float[] pred, float label) {
        throw new GBDTException("LogLossMetric should be used for binary-label classification");
    }

    public static LogLossMetric getInstance() {
        if (instance == null)
            instance = new LogLossMetric();
        return instance;
    }
}
