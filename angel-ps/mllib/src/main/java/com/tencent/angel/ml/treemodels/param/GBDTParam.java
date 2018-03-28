package com.tencent.angel.ml.treemodels.param;

import com.tencent.angel.ml.utils.Maths;

public class GBDTParam extends RegTParam {
    public int numTree;  // number of trees
    public int numThread;  // parallelism
    public int batchNum;  // number of batch in mini-batch histogram building

    public float minChildWeight;  // minimum amout of hessian (weight) allowed for a child
    public float regAlpha;  // L1 regularization factor
    public float regLambda;  // L2 regularization factor
    public float maxLeafWeight; // maximum leaf weight, default 0 means no constraints

    /**
     * Calculate leaf weight given the statistics
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return weight
     */
    public float calcWeight(float sumGrad, float sumHess) {
        if (sumHess < minChildWeight) {
            return 0.0f;
        }
        float dw;
        if (regAlpha == 0.0f) {
            dw = -sumGrad / (sumHess + regLambda);
        } else {
            dw = -Maths.thresholdL1(sumGrad, regAlpha) / (sumHess + regLambda);
        }
        if (maxLeafWeight != 0.0f) {
            if (dw > maxLeafWeight) {
                dw = maxLeafWeight;
            } else if (dw < -maxLeafWeight) {
                dw = -maxLeafWeight;
            }
        }
        return dw;
    }

    /**
     * Calculate the cost of loss function
     *
     * @param sumGrad sum of gradient values
     * @param sumHess sum of hessian values
     * @return loss gain
     */
    public float calcGain(float sumGrad, float sumHess) {
        if (sumHess < minChildWeight) {
            return 0.0f;
        }
        if (maxLeafWeight == 0.0f) {
            if (regAlpha == 0.0f) {
                return (sumGrad / (sumHess + regLambda)) * sumGrad;
            } else {
                return Maths.sqr(Maths.thresholdL1(sumGrad, regAlpha)) / (sumHess + regLambda);
            }
        } else {
            float w = calcWeight(sumGrad, sumHess);
            float ret = sumGrad * w + 0.5f * (sumHess + regLambda) * Maths.sqr(w);
            if (regAlpha == 0.0f) {
                return -2.0f * ret;
            } else {
                return -2.0f * (ret + regAlpha * Math.abs(w));
            }
        }
    }

}
