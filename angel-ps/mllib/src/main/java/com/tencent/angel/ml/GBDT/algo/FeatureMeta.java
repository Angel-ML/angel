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
package com.tencent.angel.ml.GBDT.algo;

import com.tencent.angel.ml.utils.Maths;
import com.tencent.angel.utils.Sort;


public class FeatureMeta {

  public int numFeature;
  public float[] minFeatures; // min values of features
  public float[] maxFeatures; // max values of features

  // get feature type, 0:empty 1:all equal 2:real
  public int type(int fid) {
    assert fid < minFeatures.length;
    float min = minFeatures[fid];
    float max = maxFeatures[fid];
    if (min == Float.MAX_VALUE)
      return 0;
    if (min == max) {
      return 1;
    } else {
      return 2;
    }
  }

  public float maxValue(int fid) {
    return maxFeatures[fid];
  }

  public float minValue(int fid) {
    return minFeatures[fid];
  }

  public int[] sampleCol(float p) {
    int size = (int) p * numFeature;
    int[] findex = new int[numFeature];
    for (int fid = 0; fid < numFeature; fid++) {
      findex[fid] = fid;
    }
    Maths.shuffle(findex);
    int[] rec = new int[size];
    System.arraycopy(findex, 0, rec, 0, rec.length);
    Sort.quickSort(rec, new int[rec.length], 0, rec.length - 1);
    return rec;
  }

  public FeatureMeta(int numFeature, float[] minFeatures, float[] maxFeatures) {
    this.numFeature = numFeature;
    this.minFeatures = minFeatures;
    this.maxFeatures = maxFeatures;
  }
}
