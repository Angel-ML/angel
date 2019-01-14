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


package com.tencent.angel.ml.GBDT.algo.RegTree;

import com.tencent.angel.ml.GBDT.algo.tree.TNode;
import com.tencent.angel.ml.GBDT.param.RegTParam;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.core.utils.Maths;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;


public class RegTree {

  private static final Log LOG = LogFactory.getLog(RegTree.class);

  public RegTParam param;
  // features used in this tree, if equals null, means use all the features without sampling
  public int[] fset;
  // node in the tree
  public List<TNode> nodes = new ArrayList<TNode>();
  // the gradient info of each instances
  public List<RegTNodeStat> stats = new ArrayList<RegTNodeStat>();

  public RegTree(RegTParam param) {
    this.param = param;
  }

  public void initTreeNodes() {
    // initialize nodes
    int maxDepth = param.maxDepth;
    int maxNode = Maths.pow(2, maxDepth) - 1;
    TNode root = new TNode();
    root.setNid(0);
    root.setParent(-1);
    nodes.add(root);
    RegTNodeStat rootStat = new RegTNodeStat(this.param);
    stats.add(rootStat);
    for (int nid = 1; nid < maxNode; nid++) {
      nodes.add(null);
      stats.add(null);
    }

  }

  public void clear() {
  }


  /**
   * get the leaf index of an instances.
   *
   * @param feat the feature vector of an instances
   * @param rootId the start node id
   * @return the leaf index
   */
  public int getLeafIndex(Vector feat, int rootId) {
    return 0;
  }

  /**
   * get the prediction of regression tree.
   *
   * @param feat the feature vector of an instances
   * @param rootId the start node id
   * @return the weight of leaf
   */
  public float predict(Vector feat, int rootId) {
    return 0.0f;
  }

}