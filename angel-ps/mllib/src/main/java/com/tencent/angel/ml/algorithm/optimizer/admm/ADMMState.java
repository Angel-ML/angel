/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.algorithm.optimizer.admm;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.SparseDummyVector;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;

public class ADMMState {

  List<LabeledData> instances;
  double[] x;
  double[] u;
  Int2IntOpenHashMap global2Local;
  int[] local2Global;
  int localFeatureNum;

  public ADMMState(List<LabeledData> instances) {
    extractFeature(instances);
    x = new double[localFeatureNum];
    u = new double[localFeatureNum];
    this.instances = instances;
  }

  private void extractFeature(List<LabeledData> instances) {
    global2Local = new Int2IntOpenHashMap();

    Int2IntOpenHashMap tempLocal2Global = new Int2IntOpenHashMap();
    localFeatureNum = 0;

    for (LabeledData instance : instances) {
      SparseDummyVector vector = (SparseDummyVector) instance.getX();
      int length = vector.getNonzero();
      int[] indices = vector.getIndices();
      for (int i = 0; i < length; i++) {
        int globalId = indices[i];

        if (!global2Local.containsKey(globalId)) {
          tempLocal2Global.put(localFeatureNum, globalId);
          global2Local.put(globalId, localFeatureNum);
          localFeatureNum++;
        }

        indices[i] = global2Local.get(globalId);
      }
    }

    local2Global = new int[localFeatureNum];
    ObjectIterator<Int2IntMap.Entry> iter = tempLocal2Global.int2IntEntrySet().fastIterator();

    while (iter.hasNext()) {
      Int2IntMap.Entry entry = iter.next();
      int key = entry.getIntKey();
      int value = entry.getIntValue();
      local2Global[key] = value;
    }
  }

}
