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
package com.tencent.angel.ml.param;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Description: hyper-parameter of tree model
 */

public class TreeParam implements TrainParam {

  private static final Log LOG = LogFactory.getLog(TreeParam.class);

  // maximum depth of the tree
  int maxDepth;
  // number of features used for tree construction
  int numFeature;
  // minimum loss change required for a split, otherwise stop split
  double minSplitLoss;
  // ----- the rest parameters are less important ----
  // default direction choice
  int defaultDirection;
  // whether we want to do sample data
  float rowSample;
  // whether to sample columns during tree construction
  float colSample;
  // whether to use histogram for split
  boolean isHist;
  // number of histogram units
  int numBins;
  // whether to print info during training.
  boolean silent;
  // ----- the rest parameters are obtained after training ----
  // total number of nodes
  int numNodes = 0;
  // number of deleted nodes */
  int numDeleted = 0;

  public void printParam() {
    LOG.info(String.format("Tree hyper-parameters------"
        + "maxdepth: %d, numFeature: %d, minSplitLoss: %f, rowSample: %f, colSample: %f", maxDepth,
      numFeature, minSplitLoss, rowSample, colSample));
  }
}
