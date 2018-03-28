package com.tencent.angel.ml.treemodels.tree.regression;

import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.tree.basic.TNodeStat;

public class RegTNodeStat extends TNodeStat {
    private float sumGrad;  // sum of gradient
    private float sumHess;  // sum of hessian

    public RegTNodeStat(float sumGrad, float sumHess) {
        this.sumGrad = sumGrad;
        this.sumHess = sumHess;
    }

    public float calcWeight(GBDTParam param) {
        nodeWeight = param.calcWeight(sumGrad, sumHess);
        return nodeWeight;
    }

    public float calcGain(GBDTParam param) {
        gain = param.calcGain(sumGrad, sumHess);
        return gain;
    }

    public float getSumGrad() {
        return sumGrad;
    }

    public float getSumHess() {
        return sumHess;
    }

    public void setSumGrad(float sumGrad) {
        this.sumGrad = sumGrad;
    }

    public void setSumHess(float sumHess) {
        this.sumHess = sumHess;
    }
}
