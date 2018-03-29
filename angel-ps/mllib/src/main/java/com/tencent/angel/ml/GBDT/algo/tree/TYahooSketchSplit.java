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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.GBDT.algo.tree;

import com.tencent.angel.ml.GBDT.algo.RegTree.RegTDataStore;
import com.yahoo.sketches.quantiles.DoublesSketch;

import java.util.*;

/**
 * Description: get candidate split value, using yahoo datasketches
 */
public class TYahooSketchSplit {

  public static float[][] getSplitValue(RegTDataStore dataStore, int splitNum) {

    int numFeature = dataStore.featureMeta.numFeature;

    DoublesSketch[] sketches = new DoublesSketch[numFeature];

    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = DoublesSketch.builder().build(); // default k=128
    }

    for (int nid = 0; nid < dataStore.numRow; nid++) {
      int[] indice = dataStore.instances.get(nid).getIndices();
      for (int i = 0; i < indice.length; i++) {
        int fid = indice[i];
        sketches[fid].update(dataStore.instances.get(nid).get(fid));
      }
    }

    float[][] splitSet = new float[numFeature][splitNum];

    for (int fid = 0; fid < numFeature; fid++) {

      // left child <= split value; right child > split value
      if (sketches[fid].getQuantile(0) > 0) {
        splitSet[fid][0] = 0.0f;
        for (int i = 1; i < splitNum; i++) {
          splitSet[fid][i] = (float) sketches[fid].getQuantile((i - 1) / (splitNum - 2));
        }
      } else if (sketches[fid].getQuantile(1) < 0) {
        splitSet[fid][splitNum - 1] = 0.0f;
        for (int i = 0; i < splitNum - 1; i++) {
          splitSet[fid][i] = (float) sketches[fid].getQuantile(i / (splitNum - 2));
        }
      } else {
        for (int i = 0; i < splitNum; i++) {
          splitSet[fid][i] = (float) sketches[fid].getQuantile(i / (splitNum - 1));
        }
      }

    }

    return splitSet;
  }

  public static float[][] getSplitValue(RegTDataStore dataStore, int splitNum,
    List<Integer> cateFeat) {

    int numFeature = dataStore.featureMeta.numFeature;

    DoublesSketch[] sketches = new DoublesSketch[numFeature];

    Map<Integer, Set<Float>> cateFeatTable = new HashMap<Integer, Set<Float>>();
    for (Integer feat : cateFeat) {
      cateFeatTable.put(feat, new HashSet<Float>());
    }

    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = DoublesSketch.builder().build(); // default k=128
    }

    for (int nid = 0; nid < dataStore.numRow; nid++) {
      int[] indice = dataStore.instances.get(nid).getIndices();
      for (int i = 0; i < indice.length; i++) {
        int fid = indice[i];
        double fvalue = dataStore.instances.get(nid).get(fid);
        if (cateFeat.contains(fid)) {
          cateFeatTable.get(fid).add((float) fvalue);
        } else {
          sketches[fid].update(fvalue);
        }
      }
    }

    // the first: minimal, the last: maximal
    float[][] splitSet = new float[numFeature][splitNum];

    // categorical features
    for (Map.Entry<Integer, Set<Float>> ent : cateFeatTable.entrySet()) {
      int fid = ent.getKey();
      int i = 1;  // the first value is 0
      for (float fvalue : ent.getValue()) {
        splitSet[fid][i++] = fvalue;
      }
    }

    // continuous features
    for (int fid = 0; fid < numFeature; fid++) {

      if (cateFeat.contains(fid))
        continue;

      // left child <= split value; right child > split value
      if (sketches[fid].getQuantile(0) > 0) {
        splitSet[fid][0] = 0.0f;
        for (int i = 1; i < splitNum; i++) {
          splitSet[fid][i] = (float) sketches[fid].getQuantile((float) (i - 1) / (splitNum - 2));
        }
      } else if (sketches[fid].getQuantile(1) < 0) {
        splitSet[fid][splitNum - 1] = 0.0f;
        for (int i = 0; i < splitNum - 1; i++) {
          splitSet[fid][i] = (float) sketches[fid].getQuantile((float) i / (splitNum - 2));
        }
      } else {
        for (int i = 0; i < splitNum; i++) {
          splitSet[fid][i] = (float) sketches[fid].getQuantile((float) i / (splitNum - 1));
        }
      }

    }

    return splitSet;
  }


}
