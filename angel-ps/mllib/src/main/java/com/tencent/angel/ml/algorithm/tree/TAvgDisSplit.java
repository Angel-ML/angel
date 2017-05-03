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

import com.tencent.angel.ml.algorithm.RegTree.DataMeta;
import com.tencent.angel.ml.algorithm.RegTree.RegTMaker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Description: get candidate split value, averaging the max value and min value
 */

public class TAvgDisSplit implements TSplitValueHelper {

  private static final Log LOG = LogFactory.getLog(TAvgDisSplit.class);
  public int splitNum;

  public TAvgDisSplit(int splitNum) {
    this.splitNum = splitNum;
  }


  /**
   * Get split value of each feature of a node in a tree. currently, we generate all the features
   * Todo: generate sampled features
   * 
   * @param dataMeta the data meta info
   * @param treeMaker the tree maker
   * @param nid the node id in the tree
   * @return the float[][] split value, one row for one feature
   */
  @Override
  public float[][] getSplitValue(DataMeta dataMeta, RegTMaker treeMaker, int nid) {
    int numFeature = dataMeta.featureMeta.numFeature;
    float[][] splitSet = new float[numFeature][splitNum];

    float[] binWidths = new float[numFeature];

    for (int fid = 0; fid < numFeature; fid++) {
      binWidths[fid] =
          (dataMeta.featureMeta.maxFeatures[fid] - dataMeta.featureMeta.minFeatures[fid])
              / (splitNum + 1);
    }

    for (int fid = 0; fid < numFeature; fid++) {
      for (int j = 0; j < splitNum; j++) {
        splitSet[fid][j] = dataMeta.featureMeta.minFeatures[fid] + binWidths[fid] * (j + 1);
      }
    }
    return splitSet;
  }

  // the minimal split value is the minimal value of feature
  // the splits do not include the maximal value of feature
  public static float[][] getSplitValue(DataMeta dataMeta, int splitNum) {

    int numFeature = dataMeta.featureMeta.numFeature;
    float[][] splitSet = new float[numFeature][splitNum];

    // 1. the average distance, (maxValue - minValue) / splitNum
    float[] binWidths = new float[numFeature];
    for (int fid = 0; fid < numFeature; fid++) {
      binWidths[fid] =
          (dataMeta.featureMeta.maxFeatures[fid] - dataMeta.featureMeta.minFeatures[fid])
              / splitNum;
    }

    // 2. calculate the candidate split value
    for (int fid = 0; fid < numFeature; fid++) {
      for (int j = 0; j < splitNum; j++) {
        splitSet[fid][j] = dataMeta.featureMeta.minFeatures[fid] + binWidths[fid] * j;
      }
    }
    return splitSet;
  }
}
