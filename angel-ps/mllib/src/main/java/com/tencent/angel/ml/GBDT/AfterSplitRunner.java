/**
 * Created by jeremyjiang on 2017/6/27.
 * School of EECS, Peking University
 * Copyright (c) All Rights Reserved
 */
package com.tencent.angel.ml.GBDT;

import com.tencent.angel.ml.RegTree.RegTNodeStat;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.tree.SplitEntry;
import com.tencent.angel.ml.tree.TNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Description:
 *
 * @author jeremyjiang
 *         Created at 2017/6/27 9:41
 */

public class AfterSplitRunner implements Runnable{

  private static final Log LOG = LogFactory.getLog(AfterSplitRunner.class);

  private final GBDTController controller;
  private final int nid; // tree node id
  private final DenseIntVector splitFeatureVec;
  private final DenseDoubleVector splitValueVec;
  private final DenseDoubleVector splitGainVec;
  private final DenseDoubleVector nodeGradStatsVec;

  public AfterSplitRunner(GBDTController controller, int nid,
      DenseIntVector splitFeatureVec, DenseDoubleVector splitValueVec,
      DenseDoubleVector splitGainVec, DenseDoubleVector nodeGradStatsVec) {
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
    LOG.info(String.format(
        "Active node[%d]: split feature[%d] value[%f], lossChg[%f], sumGrad[%f], sumHess[%f]",
        nid, splitFeature, splitValue, splitGain, nodeSumGrad, nodeSumHess));
    if (splitFeature != -1) {
      // 5.1. set the children nodes of this node
      this.controller.forest[this.controller.currentTree].nodes.get(nid).setLeftChild(2 * nid + 1);
      this.controller.forest[this.controller.currentTree].nodes.get(nid).setRightChild(2 * nid + 2);
      // 5.2. set split info and grad stats to this node
      SplitEntry splitEntry = new SplitEntry(splitFeature, splitValue, splitGain);
      this.controller.forest[this.controller.currentTree].stats.get(nid).setSplitEntry(splitEntry);
      this.controller.forest[this.controller.currentTree].stats.get(nid).lossChg = splitGain;
      this.controller.forest[this.controller.currentTree].stats.get(nid).setStats(nodeSumGrad, nodeSumHess);
      // 5.2. create children nodes
      TNode leftChild = new TNode(2 * nid + 1, nid, -1, -1);
      TNode rightChild = new TNode(2 * nid + 2, nid, -1, -1);
      this.controller.forest[this.controller.currentTree].nodes.set(2 * nid + 1, leftChild);
      this.controller.forest[this.controller.currentTree].nodes.set(2 * nid + 2, rightChild);
      // 5.3. create node stats for children nodes, and add them to the tree
      RegTNodeStat leftChildStat = new RegTNodeStat(this.controller.param);
      RegTNodeStat rightChildStat = new RegTNodeStat(this.controller.param);
      float leftChildSumGrad = (float) nodeGradStatsVec.get(2 * nid + 1);
      float rightChildSumGrad = (float) nodeGradStatsVec.get(2 * nid + 2);
      float leftChildSumHess = (float) nodeGradStatsVec.get(2 * nid + 1 + this.controller.maxNodeNum);
      float rightChildSumHess = (float) nodeGradStatsVec.get(2 * nid + 2 + this.controller.maxNodeNum);
      leftChildStat.setStats(leftChildSumGrad, leftChildSumHess);
      rightChildStat.setStats(rightChildSumGrad, rightChildSumHess);
      this.controller.forest[this.controller.currentTree].stats.set(2 * nid + 1, leftChildStat);
      this.controller.forest[this.controller.currentTree].stats.set(2 * nid + 2, rightChildStat);
      // 5.4. reset instance position
      this.controller.resetInsPos(nid, splitFeature, splitValue);
      // 5.5. add new active nodes if possible, inc depth, otherwise finish this tree
      if (this.controller.currentDepth < this.controller.param.maxDepth - 1) {
        LOG.debug(String.format("Add children nodes of node[%d]:[%d][%d] to active nodes",
            nid, 2 * nid + 1, 2 * nid + 2));
        this.controller.addActiveNode(2 * nid + 1);
        this.controller.addActiveNode(2 * nid + 2);
      } else {
        // 5.6. set children nodes to leaf nodes
        LOG.debug(String.format("Set children nodes of node[%d]:[%d][%d] to leaf nodes",
            nid, 2 * nid + 1, 2 * nid + 2));
        this.controller.setNodeToLeaf(2 * nid + 1, leftChildStat.baseWeight);
        this.controller.setNodeToLeaf(2 * nid + 2, rightChildStat.baseWeight);
      }
    } else {
      // 5.7. set nid to leaf node
      this.controller.setNodeToLeaf(nid, this.controller.param.calcWeight(nodeSumGrad, nodeSumHess));
    }
    // 5.8. deactivate active node
    this.controller.resetActiveTNodes(nid);
    this.controller.activeNodeStat[nid] = 0;
  }
}
