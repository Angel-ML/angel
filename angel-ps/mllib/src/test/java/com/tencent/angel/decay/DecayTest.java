package com.tencent.angel.decay;

import com.tencent.angel.ml.core.optimizer.decayer.*;
import org.junit.Test;

import java.util.Arrays;

public class DecayTest {
    private double eta = 1.0;

    @Test
    public void standardDecayTest() {
        StandardDecay decay = new StandardDecay(eta, 0.05);
        calNext(decay);
    }

    @Test
    public void WarmRestartsTest() {
        WarmRestarts decay = new WarmRestarts(eta, 0.001, 0.05);
        calNext(decay);
    }

    private void calNext(StepSizeScheduler scheduler) {
        int len = 300;
        double[] data = new double[len];

        for (int i=0; i< len; i++) {
            data[i] = scheduler.next();
        }

        System.out.println(Arrays.toString(data));
    }
}
