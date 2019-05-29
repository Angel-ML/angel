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

package com.tencent.angel.graph.collection;

import java.util.ArrayList;
import java.util.List;

public class AliasMethod {
    private final Float[] probs;
    private final Integer[] alias;

    public AliasMethod(List<Float> weights) {
        int size = weights.size();
        probs = new Float[size];
        alias = new Integer[size];

        List<Integer> large = new ArrayList<>();
        List<Integer> small = new ArrayList<>();
        float avg = 1.0f / size;
        for (int i = 0; i < size; i++) {
            if (weights.get(i) > avg) {
                large.add(i);
            } else {
                small.add(i);
            }
        }

        List<Float> weightsTemp = new ArrayList<>(weights);
        while (large.size() > 0 && small.size() > 0) {
            int less = small.remove(small.size() - 1);
            int more = large.remove(large.size() - 1);
            probs[less] = weightsTemp.get(less) * size;
            alias[less] = more;
            weightsTemp.set(more, weightsTemp.get(more) + weightsTemp.get(less) - avg);
            if (weightsTemp.get(more) > avg) {
                large.add(more);
            } else {
                small.add(more);
            }
        }

        while (small.size() > 0) {
            int less = small.remove(small.size() - 1);
            probs[less] = 1.0f;
        }

        while (large.size() > 0) {
            int more = large.remove(large.size() - 1);
            probs[more] = 1.0f;
        }
    }

    public int next() {
        int column = nextLong(probs.length);
        boolean coinToss = Math.random() < probs[column];
        return coinToss ? column : alias[column];
    }

    private static int nextLong(int n) {
        return (int) Math.floor(Math.random() * n);
    }
}
