package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.GBDTParam;

public class GradPair {
    private float grad;  // gradient value
    private float hess;  // hessian value

    public GradPair() {}

    public GradPair(float grad, float hess) {
        this.grad = grad;
        this.hess = hess;
    }

    public float getGrad() {
        return grad;
    }

    public float getHess() {
        return hess;
    }

    public void setGrad(float grad) {
        this.grad = grad;
    }

    public void setHess(float hess) {
        this.hess = hess;
    }

    public void update(float sumGrad, float sumHess) {
        this.grad = sumGrad;
        this.hess = sumHess;
    }

    public void add(float grad, float hess) {
        this.grad += grad;
        this.hess += hess;
    }

    public void add(GradPair p) {
        this.add(p.grad, p.hess);
    }

    public float calcWeight(GBDTParam param) {
        return param.calcWeight(grad, hess);
    }

    public float calcGain(GBDTParam param) {
        return param.calcGain(grad, hess);
    }
}
