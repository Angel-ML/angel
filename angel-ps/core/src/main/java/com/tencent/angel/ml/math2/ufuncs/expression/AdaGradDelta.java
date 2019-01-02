package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class AdaGradDelta extends Binary {
    private double lambda;
    private double eta;

    public AdaGradDelta(boolean inplace, double lambda, double eta) {
        setInplace(inplace);
        setKeepStorage(Constant.keepStorage);
        this.lambda = lambda;
        this.eta = eta;
    }

    @Override
    public OpType getOpType() {
        return OpType.INTERSECTION;
    }

    @Override
    public double apply(double ele1, double ele2) {
        return ele1 * eta / (Math.sqrt(ele2) + lambda * eta);
    }

    @Override
    public double apply(double ele1, float ele2) {
        return ele1 * eta / (Math.sqrt(ele2) + lambda * eta);
    }

    @Override
    public double apply(double ele1, long ele2) {
        return ele1 * eta / (Math.sqrt(ele2) + lambda * eta);
    }

    @Override
    public double apply(double ele1, int ele2) {
        return ele1 * eta / (Math.sqrt(ele2) + lambda * eta);
    }

    @Override
    public float apply(float ele1, float ele2) {
        return (float)(ele1 * eta / (Math.sqrt(ele2) + lambda * eta));
    }

    @Override
    public float apply(float ele1, long ele2) {
        return (float)(ele1 * eta / (Math.sqrt(ele2) + lambda * eta));
    }

    @Override
    public float apply(float ele1, int ele2) {
        return (float)(ele1 * eta / (Math.sqrt(ele2) + lambda * eta));
    }

    @Override
    public long apply(long ele1, long ele2) {
        return (long)(ele1 * eta / (Math.sqrt(ele2) + lambda * eta));
    }

    @Override
    public long apply(long ele1, int ele2) {
        return (long)(ele1 * eta / (Math.sqrt(ele2) + lambda * eta));
    }

    @Override
    public int apply(int ele1, int ele2) {
        return (int)(ele1 * eta / (Math.sqrt(ele2) + lambda * eta));
    }
}
