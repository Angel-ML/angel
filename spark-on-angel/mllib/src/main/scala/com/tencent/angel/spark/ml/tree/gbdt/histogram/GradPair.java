package com.tencent.angel.spark.ml.tree.gbdt.histogram;

import com.tencent.angel.spark.ml.tree.tree.param.GBDTParam;

public interface GradPair {
    void plusBy(GradPair gradPair);

    void subtractBy(GradPair gradPair);

    GradPair plus(GradPair gradPair);

    GradPair subtract(GradPair gradPair);

    void timesBy(double x);

    float calcGain(GBDTParam param);

    boolean satisfyWeight(GBDTParam param);

    GradPair copy();

}
