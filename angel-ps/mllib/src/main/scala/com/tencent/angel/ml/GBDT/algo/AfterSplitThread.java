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


package com.tencent.angel.ml.GBDT.algo;

import com.tencent.angel.ml.GBDT.algo.RegTree.RegTNodeStat;
import com.tencent.angel.ml.GBDT.algo.tree.SplitEntry;
import com.tencent.angel.ml.GBDT.algo.tree.TNode;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AfterSplitThread implements Runnable {

  private static final Log LOG = LogFactory.getLog(AfterSplitThread.class);

  private final GBDTController controller;
  private final int nid; // tree node id
  private final IntIntVector splitFeatureVec;
  private final IntDoubleVector splitValueVec;
  private final IntDoubleVector splitGainVec;
  private final IntDoubleVector nodeGradStatsVec;

  public AfterSplitThread(GBDTController controller, int nid, IntIntVector splitFeatureVec,
    IntDoubleVector splitValueVec, IntDoubleVector splitGainVec, IntDoubleVector nodeGradStatsVec) {
    this.controller = controller;
    this.nid = nid;
    this.splitFeatureVec = splitFeatureVec;
    this.splitValueVec = splitValueVec;
    this.splitGainVec = splitGainVec;
    this.nodeGradStatsVec = nodeGradStatsVec;
  }

  @Override public void run() {
    int splitFeature = splitFeatureVec.get(nid);
    float splitValue = (float) splitValueVec.get(nid);
    float splitGain = (float) splitGainVec.get(nid);
    float nodeSumGrad = (float) nodeGradStatsVec.get(nid);
    float nodeSumHess = (float) nodeGradStatsVec.get(nid + this.controller.maxNodeNum);
    LOG.info(String
      .format("Active node[%d]: split feature[%d] value[%f], lossChg[%f], sumGrad[%f], sumHess[%f]",
        nid, splitFeature, splitValue, splitGain, nodeSumGrad, nodeSumHess));
    if (splitFeature != -1) {
      // 5.1. set the children nodes of this node
      this.controller.forest[this.controller.currentTree].nodes.get(nid).setLeftChild(2 * nid + 1);
      this.controller.forest[this.controller.currentTree].nodes.get(nid).setRightChild(2 * nid + 2);
      // 5.2. set split info and grad stats to this node
      SplitEntry splitEntry = new SplitEntry(splitFeature, splitValue, splitGain);
      this.controller.forest[this.controller.currentTree].stats.get(nid).setSplitEntry(splitEntry);
      this.controller.forest[this.controller.currentTree].stats.get(nid).lossChg = splitGain;
      this.controller.forest[this.controller.currentTree].stats.get(nid)
        .setStats(nodeSumGrad, nodeSumHess);
      // 5.2. create children nodes
      TNode leftChild = new TNode(2 * nid + 1, nid, -1, -1);
      TNode rightChild = new TNode(2 * nid + 2, nid, -1, -1);
      this.controller.forest[this.controller.currentTree].nodes.set(2 * nid + 1, leftChild);
      this.controller.forest[this.controller.currentTree].nodes.set(2 * nid + 2, rightChild);
      LOG.debug(String.format("Add children nodes: %d and %d", 2 * nid + 1, 2 * nid + 2));
      // 5.3. create node stats for children nodes, and add them to the tree
      RegTNodeStat leftChildStat = new RegTNodeStat(this.controller.param);
      RegTNodeStat rightChildStat = new RegTNodeStat(this.controller.param);
      float leftChildSumGrad = (float) nodeGradStatsVec.get(2 * nid + 1);
      float rightChildSumGrad = (float) nodeGradStatsVec.get(2 * nid + 2);
      float leftChildSumHess =
        (float) nodeGradStatsVec.get(2 * nid + 1 + this.controller.maxNodeNum);
      float rightChildSumHess =
        (float) nodeGradStatsVec.get(2 * nid + 2 + this.controller.maxNodeNum);
      leftChildStat.setStats(leftChildSumGrad, leftChildSumHess);
      rightChildStat.setStats(rightChildSumGrad, rightChildSumHess);
      this.controller.forest[this.controller.currentTree].stats.set(2 * nid + 1, leftChildStat);
      this.controller.forest[this.controller.currentTree].stats.set(2 * nid + 2, rightChildStat);
      // 5.4. reset instance position
      this.controller.updateTrainInsPos(this.nid, splitFeature, splitValue);
      // 5.5. add new active nodes if possible, inc depth, otherwise finish this tree
      if (this.controller.currentDepth < this.controller.param.maxDepth - 1) {
        LOG.debug(String
          .format("Add children nodes of node[%d]:[%d][%d] to active nodes", nid, 2 * nid + 1,
            2 * nid + 2));
        this.controller.addActiveNode(2 * nid + 1);
        this.controller.addActiveNode(2 * nid + 2);
      } else {
        // 5.6. set children nodes to leaf nodes
        LOG.debug(String
          .format("Set children nodes of node[%d]:[%d][%d] to leaf nodes", nid, 2 * nid + 1,
            2 * nid + 2));
        this.controller.setNodeToLeaf(2 * nid + 1, leftChildStat.baseWeight);
        this.controller.setNodeToLeaf(2 * nid + 2, rightChildStat.baseWeight);
      }
    } else {
      // 5.7. set nid to leaf node
      this.controller
        .setNodeToLeaf(nid, this.controller.param.calcWeight(nodeSumGrad, nodeSumHess));
      LOG.debug(String.format("Set node %d to leaf", nid));
    }
    // 5.8. deactivate active node
    this.controller.resetActiveTNodes(nid);
  }
}
