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
package com.tencent.angel.ml.algorithm.RegTree;

import com.tencent.angel.ml.algorithm.param.RegTTrainParam;
import com.tencent.angel.ml.algorithm.utils.MathUtils;
import com.tencent.angel.ml.algorithm.tree.TNode;
import com.tencent.angel.ml.math.TAbstractVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: A regression tree
 */
public class RegTree {

  private static final Log LOG = LogFactory.getLog(RegTree.class);

  public RegTTrainParam param;
  // features used in this tree
  public int[] fset;
  // node in the tree
  public List<TNode> nodes = new ArrayList<TNode>();
  // the gradient info of each instances
  public List<RegTNodeStat> stats = new ArrayList<RegTNodeStat>();

  public RegTree(RegTTrainParam param) {
    this.param = param;
  }

  public void init(DataMeta dataMeta, List<GradPair> gradPairs) {
    // initialize feature id
    if (param.colSample < 1) {
      fset = dataMeta.featureMeta.sampleCol(param.colSample);
    } else {
      fset = new int[dataMeta.featureMeta.numFeature];
      for (int fid = 0; fid < fset.length; fid++) {
        fset[fid] = fid;
      }
    }
    dataMeta.featureMeta.sampleCol(param.colSample);
    // initialize nodes
    int maxDepth = param.maxDepth;
    int maxNode = MathUtils.pow(2, maxDepth) - 1;
    for (int nid = 0; nid < maxNode; nid++) {
      nodes.add(null);
      stats.add(null);
    }
    // add root node, creete split entry
    TNode root = new TNode(0, -1, -1, -1);
    // initialize statistic of the root, including gradient stats
    RegTNodeStat rootStat = new RegTNodeStat(param, gradPairs);
    nodes.set(0, root);
    stats.set(0, rootStat);
  }

  public void clear() {}


  /**
   * get the leaf index of an instances.
   *
   * @param feat the feature vector of an instances
   * @param rootId the start node id
   * @return the leaf index
   */
  public int getLeafIndex(TAbstractVector feat, int rootId) {
    return 0;
  }

  /**
   * get the prediction of regression tree.
   *
   * @param feat the feature vector of an instances
   * @param rootId the start node id
   * @return the weight of leaf
   */
  public float predict(TAbstractVector feat, int rootId) {
    return 0.0f;
  }
}
