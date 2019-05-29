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

public class CompactWeightedCollection<T> implements WeightedCollection<T> {
    private List<T> ids;
    private List<Float> sumWeights;
    private float sumWeight;

    @Override
    public void init(List<IdWeightPair<T>> idWeights) {
        int size = idWeights.size();
        ids = new ArrayList<>(size);
        sumWeights = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            IdWeightPair<T> idWeight = idWeights.get(i);
            ids.add(idWeight.getId());
            sumWeight += idWeight.getWeight();
            sumWeights.add(sumWeight);
        }
    }

    @Override
    public void init(T[] ids, Float[] weights) {
        assert ids.length == weights.length;

        int size = ids.length;
        this.ids = new ArrayList<>(size);
        sumWeights = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            this.ids.add(ids[i]);
            sumWeight += weights[i];
            sumWeights.add(sumWeight);
        }
    }

    @Override
    public IdWeightPair<T> sample() {
        int mid = randomSelect(sumWeights, 0, ids.size() - 1);
        float preSumWeight = 0;
        if (mid > 0) {
            preSumWeight = sumWeights.get(mid - 1);
        }
        return new IdWeightPair<>(ids.get(mid), sumWeights.get(mid) - preSumWeight);
    }

    @Override
    public int size() {
        return ids.size();
    }

    @Override
    public IdWeightPair<T> get(int idx) {
        float preSumWeight = 0;
        if (idx > 0) {
            preSumWeight = sumWeights.get(idx);
        }
        return new IdWeightPair<>(ids.get(idx), sumWeights.get(idx) - preSumWeight);
    }

    @Override
    public float getSumWeight() {
        return sumWeight;
    }

    public static int randomSelect(List<Float> sumWeights, int beginPos, int endPos) {
        float limitBegin = beginPos == 0 ? 0 : sumWeights.get(beginPos - 1);
        float limitEnd = sumWeights.get(endPos);
        float r = (float) Math.random() * (limitEnd - limitBegin) + limitBegin;

        int low = beginPos, high = endPos, mid = 0;
        boolean finish = false;
        while (low <= high && !finish) {
            mid = (low + high) / 2;
            float intervalBegin = mid == 0 ? 0 : sumWeights.get(mid - 1);
            float intervalEnd = sumWeights.get(mid);
            if (intervalBegin <= r && r < intervalEnd) {
                finish = true;
            } else if (intervalBegin > r) {
                high = mid - 1;
            } else if (intervalEnd <= r) {
                low = mid + 1;
            }
        }

        return mid;
    }
}
