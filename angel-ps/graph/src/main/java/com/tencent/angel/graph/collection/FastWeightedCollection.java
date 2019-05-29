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

import com.tencent.angel.graph.data.IdWeightPair;

import java.util.ArrayList;
import java.util.List;

public class FastWeightedCollection<T> implements WeightedCollection<T> {
    private List<T> ids;
    private List<Float> weights;
    private float sumWeight;
    private AliasMethod aliasMethod;

    @Override
    public void init(List<IdWeightPair<T>> idWeights) {
        int size = idWeights.size();
        ids = new ArrayList<>(size);
        weights = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            IdWeightPair<T> idWeight = idWeights.get(i);
            ids.add(idWeight.getId());
            weights.add(idWeight.getWeight());
            sumWeight += idWeight.getWeight();
        }

        List<Float> normWeights = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            normWeights.add(weights.get(i) / sumWeight);
        }
        aliasMethod = new AliasMethod(normWeights);
    }

    @Override
    public void init(T[] ids, Float[] weights) {
        assert ids.length == weights.length;

        int size = ids.length;
        this.ids = new ArrayList<>(size);
        this.weights = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            this.ids.add(ids[i]);
            this.weights.add(weights[i]);
            sumWeight += weights[i];
        }

        List<Float> normWeights = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            normWeights.add(weights[i] / sumWeight);
        }
        aliasMethod = new AliasMethod(normWeights);
    }

    @Override
    public IdWeightPair<T> sample() {
        int column = aliasMethod.next();
        return new IdWeightPair<>(ids.get(column), weights.get(column));
    }

    @Override
    public int size() {
        return null != ids ? ids.size() : 0;
    }

    @Override
    public IdWeightPair<T> get(int i) {
        return new IdWeightPair<>(ids.get(i), weights.get(i));
    }

    @Override
    public float getSumWeight() {
        return sumWeight;
    }
}
