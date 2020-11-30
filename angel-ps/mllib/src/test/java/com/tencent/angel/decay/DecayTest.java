/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
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
