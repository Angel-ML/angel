package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class AdaDeltaHessian extends Binary {
    private double esp = 1e-7;

    public AdaDeltaHessian(boolean inplace) {
        setInplace(inplace);
        setKeepStorage(Constant.keepStorage);
    }

    @Override
    public OpType getOpType() {
        return OpType.INTERSECTION;
    }

    @Override
    public double apply(double ele1, double ele2) {
        return Math.sqrt((ele1 + esp) / (ele2 + esp));
    }

    @Override
    public double apply(double ele1, float ele2) {
        return Math.sqrt((ele1 + esp) / (ele2 + esp));
    }

    @Override
    public double apply(double ele1, long ele2) {
        return Math.sqrt((ele1 + esp) / (ele2 + esp));
    }

    @Override
    public double apply(double ele1, int ele2) {
        return Math.sqrt((ele1 + esp) / (ele2 + esp));
    }

    @Override
    public float apply(float ele1, float ele2) {
        return (float) (Math.sqrt((ele1 + esp) / (ele2 + esp)));
    }

    @Override
    public float apply(float ele1, long ele2) {
        return (float) (Math.sqrt((ele1 + esp) / (ele2 + esp)));
    }

    @Override
    public float apply(float ele1, int ele2) {
        return (float) (Math.sqrt((ele1 + esp) / (ele2 + esp)));
    }

    @Override
    public long apply(long ele1, long ele2) {
        return (long) (Math.sqrt((ele1 + esp) / (ele2 + esp)));
    }

    @Override
    public long apply(long ele1, int ele2) {
        return (long) (Math.sqrt((ele1 + esp) / (ele2 + esp)));
    }

    @Override
    public int apply(int ele1, int ele2) {
        return (int) (Math.sqrt((ele1 + esp) / (ele2 + esp)));
    }
}
