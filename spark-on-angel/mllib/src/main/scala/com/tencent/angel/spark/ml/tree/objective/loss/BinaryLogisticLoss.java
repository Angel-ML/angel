package com.tencent.angel.spark.ml.tree.objective.loss;

import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric;
import com.tencent.angel.spark.ml.tree.util.Maths;
import javax.inject.Singleton;

@Singleton
public class BinaryLogisticLoss implements BinaryLoss {
    private static BinaryLogisticLoss instance;

    private BinaryLogisticLoss() {}

    @Override
    public Kind getKind() {
        return Kind.BinaryLogistic;
    }

    @Override
    public EvalMetric.Kind defaultEvalMetric() {
        return EvalMetric.Kind.LOG_LOSS;
    }

    @Override
    public double firOrderGrad(float pred, float label) {
        double prob = Maths.fastSigmoid(pred);
        return prob - label;
    }

    @Override
    public double secOrderGrad(float pred, float label) {
        double prob = Maths.fastSigmoid(pred);
        return Math.max(prob * (1 - prob), Maths.EPSILON);
    }

    @Override
    public double secOrderGrad(float pred, float label, double firGrad) {
        double prob = firGrad + label;
        return Math.max(prob * (1 - prob), Maths.EPSILON);
    }

    public static BinaryLogisticLoss getInstance() {
        if (instance == null)
            instance = new BinaryLogisticLoss();
        return instance;
    }
}
