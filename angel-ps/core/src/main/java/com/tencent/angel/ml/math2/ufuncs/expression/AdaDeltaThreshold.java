package com.tencent.angel.ml.math2.ufuncs.expression;

import com.tencent.angel.ml.math2.utils.Constant;

public class AdaDeltaThreshold extends Binary {
    private double lambda1;
    private double lambda2;

    public AdaDeltaThreshold(boolean inplace, double lambda1, double lambda2) {
        setInplace(inplace);
        setKeepStorage(Constant.keepStorage);
        this.lambda1 = lambda1;
        this.lambda2 = lambda2;
    }

    @Override
    public OpType getOpType() {
        return OpType.INTERSECTION;
    }

    @Override
    public double apply(double ele1, double ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return ele1 - threshold;
        } else if (ele1 < -threshold) {
            return ele1 + threshold;
        } else {
            return 0;
        }
    }

    @Override
    public double apply(double ele1, float ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return ele1 - threshold;
        } else if (ele1 < -threshold) {
            return ele1 + threshold;
        } else {
            return 0;
        }
    }

    @Override
    public double apply(double ele1, long ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return ele1 - threshold;
        } else if (ele1 < -threshold) {
            return ele1 + threshold;
        } else {
            return 0;
        }
    }

    @Override
    public double apply(double ele1, int ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return ele1 - threshold;
        } else if (ele1 < -threshold) {
            return ele1 + threshold;
        } else {
            return 0;
        }
    }

    @Override
    public float apply(float ele1, float ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return (float) (ele1 - threshold);
        } else if (ele1 < -threshold) {
            return (float) (ele1 + threshold);
        } else {
            return 0;
        }
    }

    @Override
    public float apply(float ele1, long ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return (float) (ele1 - threshold);
        } else if (ele1 < -threshold) {
            return (float) (ele1 + threshold);
        } else {
            return 0;
        }
    }

    @Override
    public float apply(float ele1, int ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return (float) (ele1 - threshold);
        } else if (ele1 < -threshold) {
            return (float) (ele1 + threshold);
        } else {
            return 0;
        }
    }

    @Override
    public long apply(long ele1, long ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return (long) (ele1 - threshold);
        } else if (ele1 < -threshold) {
            return (long) (ele1 + threshold);
        } else {
            return 0;
        }
    }

    @Override
    public long apply(long ele1, int ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return (long) (ele1 - threshold);
        } else if (ele1 < -threshold) {
            return (long) (ele1 + threshold);
        } else {
            return 0;
        }
    }

    @Override
    public int apply(int ele1, int ele2) {
        double threshold = lambda1 / (Math.sqrt(ele2) + lambda2);
        if (ele1 > threshold) {
            return (int) (ele1 - threshold);
        } else if (ele1 < -threshold) {
            return (int) (ele1 + threshold);
        } else {
            return 0;
        }
    }
}
