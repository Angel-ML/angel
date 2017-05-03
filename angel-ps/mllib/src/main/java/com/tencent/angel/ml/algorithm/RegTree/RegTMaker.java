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

import com.tencent.angel.ml.algorithm.objective.RegLossObj;
import com.tencent.angel.ml.algorithm.tree.SplitEntry;
import com.tencent.angel.ml.algorithm.tree.TAvgDisSplit;
import com.tencent.angel.ml.algorithm.utils.MathUtils;
import com.tencent.angel.ml.algorithm.objective.Loss;
import com.tencent.angel.ml.algorithm.objective.LossHelper;
import com.tencent.angel.ml.algorithm.objective.ObjFunc;
import com.tencent.angel.ml.algorithm.param.RegTTrainParam;
import com.tencent.angel.ml.algorithm.tree.TNode;
import com.tencent.angel.ml.algorithm.tree.TSplitValueHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class RegTMaker extends TreeMaker {

  private static final Log LOG = LogFactory.getLog(RegTMaker.class);

  public RegTTrainParam param;
  public RegTree regTree;
  public DataMeta dataMeta;
  // queue of nodes to be expanded, -1 means no work
  private List<Integer> qexpend = new ArrayList<Integer>();
  // map active node to its working index offset in qexpand
  // can be -1, which means the node is not actively expanding
  private Map<Integer, Integer> node2Work = new HashMap<Integer, Integer>();
  // position of each instances in the tree
  // can be negative, which means this ins2Node is no longer expanding
  public List<Integer> ins2Node = new ArrayList<Integer>();
  // used to candidate split cut value
  private TSplitValueHelper splitValueHelper;
  // HistSet of all nodes, use node2Work to find the position
  private List<HistSet> hsets = new ArrayList<HistSet>();
  // loss function
  private ObjFunc objfunc;
  // gradient statistics
  public List<GradPair> gradPairs = new ArrayList<>();

  public RegTMaker(RegTTrainParam param, DataMeta dataMeta, RegTree regTree) {
    this.param = param;
    this.regTree = regTree;
    this.dataMeta = dataMeta;
  }

  public void clear() {
    param = null;
    regTree = null;
    dataMeta = null;
    qexpend.clear();
    node2Work.clear();
    ins2Node.clear();
    hsets.clear();
    gradPairs.clear();
  }

  /**
   * Initialize. // initialize feature id, add root node, calculate grad statistic, set instances
   * ins2Node, add node to work queue
   */
  public void init() {
    LOG.info("------Regression tree initialize------");
    // create loss function
    LossHelper loss = new Loss.LogisticClassification();
    objfunc = new RegLossObj(loss);
    // calculate gradient info
    calGradPairs();
    // add root node, including split entry
    regTree.init(dataMeta, gradPairs);
    // add root node work
    addWork2Queue(0);
    // create split value helper
    splitValueHelper = new TAvgDisSplit(param.numSplit);
    // init instance position to root
    for (int insIdx = 0; insIdx < dataMeta.numRow; insIdx++) {
      ins2Node.add(0);
    }
    // init histogram
  }

  private boolean isFinished() {
    return !isActiveWork();
  }

  // calculate grad info of each instance
  private void calGradPairs() {
    LOG.info("------Calculate grad pairs------");
    gradPairs = objfunc.getGradient(dataMeta.preds, dataMeta, 0);
    for (int insIdx = 0; insIdx < dataMeta.numRow; insIdx++) {
      LOG.info(String.format("Instance[%d]: gradient[%f], hessien[%f]", insIdx,
          gradPairs.get(insIdx).getGrad(), gradPairs.get(insIdx).getHess()));
    }
  }

  // add active work(node) to queue
  synchronized private void addWork2Queue(int nid) {
    LOG.info("------Add node " + nid + " to the queue------");
    // add new work to the queue
    qexpend.add(nid);
    // init histogram
    HistSet histSet = new HistSet(regTree, param, nid);
    hsets.add(histSet);
    node2Work.put(nid, qexpend.size() - 1);
    LOG.info(String.format("Node[%d] is in queue[%d]", nid, node2Work.get(nid)));
  }

  // get one work(node) from queue, -1 means no active work
  private int getOneWork() {
    LOG.info("------Get active work from " + qexpend.toString() + "------");
    for (int nid : qexpend) {
      if (nid != -1) {
        LOG.info("Get one work of node " + nid);
        return nid;
      }
    }
    LOG.info("No active work");
    return -1;
  }

  // get candidate split values
  private float[][] getSplitValue(DataMeta dataMeta, int nid) {
    return splitValueHelper.getSplitValue(dataMeta, this, nid);
  }

  // build gradient histogram
  private void buildHistogram(int nid) {
    LOG.info(String.format("------Build histogram of node[%d]------", nid));
    // get candidate split value
    float[][] candidSplitValue = getSplitValue(dataMeta, nid);
    int wid = node2Work.get(nid); // node's ins2Node in the queue
    HistSet histSet = hsets.get(wid);
    histSet.build(this, dataMeta, candidSplitValue);
  }

  // find the best split from candidates, add new node to tree
  private SplitEntry findBestSplit(int nid) {
    LOG.info(String.format("------To find the best split of node[%d]------", nid));
    SplitEntry splitEntry = new SplitEntry();
    GradStats bestLeftStat = new GradStats();
    GradStats bestRightStat = new GradStats();
    int wid = node2Work.get(nid); // work id
    HistSet hset = hsets.get(wid); // histogram of the node
    Integer[] feaSet = hset.fea2Unit.keySet().toArray(new Integer[0]); // feature set
    LOG.info(String.format("The best split before looping the histogram: fid[%d], fvalue[%f]",
        splitEntry.fid, splitEntry.fvalue));
    for (int fid : feaSet) {
      int unitIdx = hset.fea2Unit.get(fid);
      HistUnit hunit = hset.units.get(unitIdx);
      hunit.findBestSplit(regTree.stats.get(nid), splitEntry, bestLeftStat, bestRightStat);
    }
    LOG.info(String.format("The best split after looping the histogram: fid[%d], fvalue[%f]",
        splitEntry.fid, splitEntry.fvalue));
    // add new node,
    if (splitEntry.fid != -1) {
      // set node's left and right children
      regTree.nodes.get(nid).setLeftChild(2 * nid + 1);
      regTree.nodes.get(nid).setRightChild(2 * nid + 2);
      // create left and right children node, add them to regtree
      TNode leftChild = new TNode(2 * nid + 1, nid, -1, -1);
      TNode rightChild = new TNode(2 * nid + 2, nid, -1, -1);
      regTree.nodes.set(2 * nid + 1, leftChild);
      regTree.nodes.set(2 * nid + 2, rightChild);
      // create node stats for children nodes add them to regtree
      RegTNodeStat leftNodeStat = new RegTNodeStat(param);
      leftNodeStat.setStats(bestLeftStat);
      RegTNodeStat rightNodeStat = new RegTNodeStat(param);
      rightNodeStat.setStats(bestRightStat);
      regTree.stats.set(2 * nid + 1, leftNodeStat);
      regTree.stats.set(2 * nid + 2, rightNodeStat);
    } else {
      chg2Leaf(nid);
    }
    return splitEntry;
  }

  // job after splits
  // update work queue, add new work to queue, update node to work
  // update instance pos,
  private void afterSplit(int nid) {
    updateQueue(nid);
    // no extra work for leaf node
    if (regTree.nodes.get(nid).isLeaf()) {
      return;
    }
    addChildWork(nid);
    updateInsPos(nid);
  }

  // update work queue, set finished node to inactive
  private void updateQueue(int nid) {
    // set node's work to inactive, set node2Work to inactivate
    int wid = node2Work.get(nid);
    qexpend.set(wid, -1);
    node2Work.put(nid, -1);
  }

  // add children node's work to queue
  private void addChildWork(int nid) {
    addWork2Queue(2 * nid + 1);
    addWork2Queue(2 * nid + 2);
  }

  // update instance ins2Node
  private void updateInsPos(int nid) {
    LOG.info("update instance position in the tree");
    SplitEntry splitEntry = regTree.stats.get(nid).splitEntry;
    int fid = splitEntry.fid;
    assert fid != -1;
    float splitValue = splitEntry.fvalue;
    // update instance's corresponding node
    for (int insIdx = 0; insIdx < ins2Node.size(); insIdx++) {
      float fvalue = (float) dataMeta.instances.get(insIdx).get(fid);
      if (fvalue <= splitValue) {
        ins2Node.set(insIdx, 2 * nid + 1);
        // LOG.info(String.format("Move ins[%d] fid[%d] fvalue[%f] to node[%d]",
        // insIdx, fid, fvalue, 2 * nid + 1));
      } else {
        ins2Node.set(insIdx, 2 * nid + 2);
        // LOG.info(String.format("Move ins[%d] fid[%d] fvalue[%f] to node[%d]",
        // insIdx, fid, fvalue, 2 * nid + 2));
      }
    }
  }

  // check whether there exist active works in the queue
  private boolean isActiveWork() {
    for (int nid : qexpend) {
      if (nid >= 0) {
        return true;
      }
    }
    return false;
  }

  // change the node to leaf
  private void chg2Leaf(int nid) {
    // change to leaf
    regTree.nodes.get(nid).chgToLeaf();
    regTree.nodes.get(nid).setLeafValue(regTree.stats.get(nid).baseWeight * param.learningRate);
  }

  // update the preds of instances
  private void updatePreds() {
    for (int insIdx = 0; insIdx < dataMeta.instances.size(); insIdx++) {
      int nid = ins2Node.get(insIdx);
      float weight = regTree.stats.get(nid).baseWeight;
      dataMeta.preds[insIdx] += weight * param.learningRate;
    }
    LOG.info("Preds: " + Arrays.toString(dataMeta.preds));
  }

  // evaluate the pre result
  private void evaluate() {
    int count = 0;
    for (int insIdx = 0; insIdx < dataMeta.instances.size(); insIdx++) {
      float label = dataMeta.labels[insIdx];
      float pred = dataMeta.preds[insIdx] > 0 ? 1.0f : 0.0f;
      if (label == pred) {
        count++;
      }
    }
    float accuracy = (float) count / (float) dataMeta.numRow;
    LOG.info("Prediction accuracy: " + accuracy);
  }

  // build the regression tree
  public void buildRegTree() {
    while (isActiveWork()) {
      LOG.info("Go to the next node");
      int nid = getOneWork(); // node id to be expand
      // if reach the max depth, set it to leaf
      if (nid >= MathUtils.pow(2, param.maxDepth - 1) - 1) {
        chg2Leaf(nid);
        updateQueue(nid);
        continue;
      }
      buildHistogram(nid);
      findBestSplit(nid);
      afterSplit(nid);
    }
    updatePreds();
    evaluate();
  }

}
