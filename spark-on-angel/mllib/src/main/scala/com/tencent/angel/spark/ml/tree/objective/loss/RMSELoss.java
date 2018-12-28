package com.tencent.angel.spark.ml.tree.objective.loss;

import com.tencent.angel.spark.ml.tree.objective.metric.EvalMetric;
import java.util.Arrays;
import javax.inject.Singleton;
import com.tencent.angel.spark.ml.tree.util.Maths;

@Singleton
public class RMSELoss implements BinaryLoss, MultiLoss {
    private static RMSELoss instance;

    private RMSELoss() {}

    @Override
    public Kind getKind() {
        return Kind.RMSE;
    }

    @Override
    public EvalMetric.Kind defaultEvalMetric() {
        return EvalMetric.Kind.RMSE;
    }

    @Override
    public double firOrderGrad(float pred, float label) {
        return pred - label;
    }

    @Override
    public double secOrderGrad(float pred, float label) {
        return 1.0;
    }

    @Override
    public double secOrderGrad(float pred, float label, double firGrad) {
        return 1.0;
    }

    @Override
    public double[] firOrderGrad(float[] pred, float label) {
        int numLabel = pred.length;
        int trueLabel = (int) label;
        double[] grad = new double[numLabel];
        for (int i = 0; i < numLabel; i++)
            grad[i] = pred[i] - (trueLabel == i ? 1 : 0);
        return grad;
    }

    @Override
    public double[] secOrderGradDiag(float[] pred, float label) {
        int numLabel = pred.length;
        double[] hess = new double[numLabel];
        Arrays.fill(hess, 1.0);
        return hess;
    }

    @Override
    public double[] secOrderGradDiag(float[] pred, float label, double[] firGrad) {
        return secOrderGradDiag(pred, label);
    }

    @Override
    public double[] secOrderGradFull(float[] pred, float label) {
        int numLabel = pred.length;
        int size = (numLabel + 1) * numLabel / 2;
        double[] hess = new double[size];
        for (int i = 0; i < numLabel; i++) {
            int t = Maths.indexOfLowerTriangularMatrix(i, i);
            hess[t] = 1.0;
        }
        return hess;
    }

    @Override
    public double[] secOrderGradFull(float[] pred, float label, double[] firGrad) {
        return secOrderGradFull(pred, label);
    }

    public static RMSELoss getInstance() {
        if (instance == null)
            instance = new RMSELoss();
        return instance;
    }
}
