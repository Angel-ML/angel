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

package com.tencent.angel.ml.algorithm.tree;

import com.tencent.angel.ml.algorithm.utils.MathUtils;
import com.tencent.angel.ml.algorithm.RegTree.DataMeta;
import com.tencent.angel.ml.algorithm.RegTree.RegTMaker;

import com.yahoo.sketches.quantiles.DoublesSketch;

/**
 * Description: get candidate split value, using yahoo datasketches
 */
public class TYahooSketchSplit implements TSplitValueHelper {
  @Override
  public float[][] getSplitValue(DataMeta dataMeta, RegTMaker treeMaker, int nid) {
    return new float[0][];
  }

  public static float[][] getSplitValue(DataMeta dataMeta, int splitNum) {

    double[] fracs = new double[splitNum];
    for (int i = 0; i < splitNum; i++) {
      fracs[i] = i / (double) splitNum;
    }

    int numFeature = dataMeta.featureMeta.numFeature;

    DoublesSketch[] sketches = new DoublesSketch[numFeature];

    for (int i = 0; i < sketches.length; i++) {
      sketches[i] = DoublesSketch.builder().build(); // default k=128
    }

    for (int nid = 0; nid < dataMeta.numRow; nid++) {
      int[] indice = dataMeta.instances.get(nid).getIndices();
      for (int i = 0; i < indice.length; i++) {
        int fid = indice[i];
        sketches[fid].update(dataMeta.instances.get(nid).get(fid));
      }
    }

    float[][] splitSet = new float[numFeature][splitNum];

    for (int fid = 0; fid < numFeature; fid++) {
      splitSet[fid] = MathUtils.double2Float(sketches[fid].getQuantiles(fracs));
    }

    return splitSet;
  }
}
