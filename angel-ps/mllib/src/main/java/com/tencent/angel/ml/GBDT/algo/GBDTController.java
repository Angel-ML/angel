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


import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.ml.GBDT.GBDTModel;
import com.tencent.angel.ml.GBDT.algo.RegTree.*;
import com.tencent.angel.ml.GBDT.algo.tree.SplitEntry;
import com.tencent.angel.ml.GBDT.algo.tree.TYahooSketchSplit;
import com.tencent.angel.ml.GBDT.psf.GBDTGradHistGetRowFunc;
import com.tencent.angel.ml.GBDT.psf.GBDTGradHistGetRowResult;
import com.tencent.angel.ml.GBDT.psf.HistAggrParam;
import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.math.vector.*;
import com.tencent.angel.ml.metric.EvalMetric;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.ml.objective.ObjFunc;
import com.tencent.angel.ml.param.GBDTParam;
import com.tencent.angel.ml.utils.Maths;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple1;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class GBDTController {

  private static final Log LOG = LogFactory.getLog(GBDTController.class);

  public TaskContext taskContext;
  public GBDTModel model;
  public GBDTParam param;
  public RegTDataStore trainDataStore;
  public RegTDataStore validDataStore;
  public RegTree[] forest;
  public int phase;
  public int clock;
  public int currentTree;
  public int currentDepth;
  public int maxNodeNum;

  // loss function
  public ObjFunc objfunc;
  // gradient and hessian
  public List<GradPair> gradPairs = new ArrayList<>();

  public float[] sketches; // size: featureNum * splitNum
  public List<Integer> cateFeatList;
    // categorical feature set, null: none, empty: all, else: partial
  public Map<Integer, Integer> cateFeatNum; // number of splits of categorical features
  public int[] fSet; // sampled features in the current tree
  public int[] activeNode; // active tree node, 1:active, 0:inactive
  public int[] activeNodeStat; // >=1:running, 0:finished, -1:failed
  public int[] instancePos; // map tree node to instance, each item is instance id
  public int[] nodePosStart; // node's start index in instancePos, size: maxNodeNum
  public int[] nodePosEnd; // node's end index in instancePos, instances in [start, end] belong to a
  // tree node

  public int[] splitFeats; // local stored split feature id
  public double[] splitValues; // local stored split feature value

  private ExecutorService threadPool;

  public GBDTController(TaskContext taskContext, GBDTParam param, RegTDataStore trainDataStore,
    RegTDataStore validDataStore, GBDTModel model) {
    this.taskContext = taskContext;
    this.param = param;
    this.trainDataStore = trainDataStore;
    this.validDataStore = validDataStore;
    this.model = model;
  }

  public void init() throws Exception {
    this.forest = new RegTree[this.param.treeNum];
    // initialize the phase
    this.phase = GBDTPhase.CREATE_SKETCH;
    this.clock = 0;
    // current tree and depth
    this.currentTree = 0; // tree starts from 0
    this.currentDepth = 1; // depth starts from 1
    // create loss function
    this.objfunc = param.getLossFunc();

    this.sketches = new float[this.param.numFeature * this.param.numSplit];

    String cateFeatStr = this.taskContext.getConf()
      .get(MLConf.ML_GBDT_CATE_FEAT(), MLConf.DEFAULT_ML_GBDT_CATE_FEAT());
    cateFeatList = new ArrayList<>();
    cateFeatNum = new HashMap<>();
    switch (cateFeatStr) {
      case "all":
        for (int fid = 0; fid < this.param.numFeature; fid++) {
          cateFeatList.add(fid);
        }
        break;
      case "none":
        break;
      default:
        String[] splits = cateFeatStr.split(",");
        for (int i = 0; i < splits.length; i++) {
          String[] fidAndNum = splits[i].split(":");
          int fid = Integer.parseInt(fidAndNum[0]);
          int num = Integer.parseInt(fidAndNum[1]);
          assert num < this.param.numSplit;
          if (!cateFeatList.contains(fid)) {
            cateFeatList.add(fid);
          }
        }
    }

    this.maxNodeNum = Maths.pow(2, this.param.maxDepth) - 1;
    this.activeNode = new int[maxNodeNum];
    this.activeNodeStat = new int[maxNodeNum];
    this.instancePos = new int[trainDataStore.numRow];
    for (int i = 0; i < this.trainDataStore.instances.size(); i++) {
      this.instancePos[i] = i;
    }
    this.nodePosStart = new int[maxNodeNum];
    this.nodePosEnd = new int[maxNodeNum];
    this.nodePosStart[0] = 0;
    this.nodePosEnd[0] = instancePos.length - 1;
    this.splitFeats = new int[maxNodeNum];
    this.splitValues = new double[maxNodeNum];
    this.threadPool = Executors.newFixedThreadPool(this.param.maxThreadNum);
  }

  private void clockAllMatrix(Set<String> needFlushMatrices, boolean wait) throws Exception {
    long startTime = System.currentTimeMillis();

    List<Future> clockFutures = new ArrayList<Future>();
    for (Map.Entry<String, PSModel> entry : model.getPSModels().entrySet()) {
      if (needFlushMatrices.contains(entry.getKey())) {
        clockFutures.add(entry.getValue().clock(true));
      } else {
        clockFutures.add(entry.getValue().clock(false));
      }
    }

    if (wait) {
      int size = clockFutures.size();
      for (int i = 0; i < size; i++) {
        clockFutures.get(i).get();
      }
    }

    LOG.info(String.format("clock and flush matrices %s cost %d ms", needFlushMatrices,
      System.currentTimeMillis() - startTime));
  }

  // calculate grad info of each instance
  private void calGradPairs() {
    LOG.info("------Calculate grad pairs------");
    gradPairs.clear();
    gradPairs.addAll(objfunc.calGrad(this.trainDataStore.preds, this.trainDataStore, 0));
    LOG.debug(String.format("Instance[%d]: label[%f], pred[%f], gradient[%f], hessien[%f]", 0,
      this.trainDataStore.labels[0], this.trainDataStore.preds[0], gradPairs.get(0).getGrad(),
      gradPairs.get(0).getHess()));
  }

  // create data sketch, push candidate split value to PS
  public void createSketch() throws Exception {
    PSModel sketch = model.getPSModel(this.param.sketchName);
    PSModel cateFeat = model.getPSModel(this.param.cateFeatureName);
    if (taskContext.getTaskIndex() == 0) {
      LOG.info("------Create sketch------");
      long startTime = System.currentTimeMillis();
      DenseDoubleVector sketchVec =
        new DenseDoubleVector(this.param.numFeature * this.param.numSplit);
      DenseDoubleVector cateFeatVec = null;
      if (!this.cateFeatList.isEmpty()) {
        cateFeatVec = new DenseDoubleVector(this.cateFeatList.size() * this.param.numSplit);
      }

      // 1. calculate candidate split value
      float[][] splits = TYahooSketchSplit
        .getSplitValue(this.trainDataStore, this.param.numSplit, this.cateFeatList);

      if (splits.length == this.param.numFeature && splits[0].length == this.param.numSplit) {
        for (int fid = 0; fid < splits.length; fid++) {
          if (cateFeatList.contains(fid)) {
            continue;
          }
          for (int j = 0; j < splits[fid].length; j++) {
            sketchVec.set(fid * this.param.numSplit + j, splits[fid][j]);
          }
        }
      } else {
        LOG.error("Incompatible sketches size.");
      }

      // categorical features
      if (!this.cateFeatList.isEmpty()) {
        Collections.sort(this.cateFeatList);
        for (int i = 0; i < this.cateFeatList.size(); i++) {
          int fid = this.cateFeatList.get(i);
          int start = i * this.param.numSplit;
          for (int j = 0; j < splits[fid].length; j++) {
            if (splits[fid][j] == 0 && j > 0)
              break;
            cateFeatVec.set(start + j, splits[fid][j]);
          }
        }
      }

      // 2. push local sketch to PS
      sketch.increment(0, sketchVec);
      if (null != cateFeatVec) {
        cateFeat.increment(this.taskContext.getTaskIndex(), cateFeatVec);
      }
      // 3. set phase to GET_SKETCH
      this.phase = GBDTPhase.GET_SKETCH;
      LOG.info(String.format("Create sketch cost: %d ms", System.currentTimeMillis() - startTime));
    }

    Set<String> needFlushMatrixSet = new HashSet<String>(1);
    needFlushMatrixSet.add(this.param.sketchName);
    needFlushMatrixSet.add(this.param.cateFeatureName);
    clockAllMatrix(needFlushMatrixSet, true);
  }

  public void mergeCateFeatSketch() throws Exception {

    LOG.info("------Merge categorical features------");

    Set<String> needFlushMatrixSet = new HashSet<String>(1);

    // the leader worker
    if (!this.cateFeatList.isEmpty() && this.taskContext.getTaskIndex() == 0) {

      PSModel cateFeat = model.getPSModel(this.param.cateFeatureName);
      PSModel sketch = model.getPSModel(this.param.sketchName);

      Set<Double>[] featSet = new HashSet[cateFeatList.size()];
      for (int i = 0; i < cateFeatList.size(); i++) {
        featSet[i] = new HashSet<>();
      }

      int workerNum = this.taskContext.getConf().getInt(AngelConf.ANGEL_WORKERGROUP_ACTUAL_NUM, 1);

      // merge categorical features
      for (int worker = 0; worker < workerNum; worker++) {
        DenseDoubleVector vec = (DenseDoubleVector) cateFeat.getRow(worker);
        for (int i = 0; i < cateFeatList.size(); i++) {
          int fid = cateFeatList.get(i);
          int start = i * this.param.numSplit;
          for (int j = 0; j < this.param.numSplit; j++) {
            double fvalue = vec.get(start + j);
            featSet[i].add(fvalue);
          }
        }
      }

      // create updates
      SparseDoubleVector cateFeatVec =
        new SparseDoubleVector(this.param.numFeature * this.param.numSplit);
      for (int i = 0; i < cateFeatList.size(); i++) {
        int fid = cateFeatList.get(i);
        int start = fid * this.param.numSplit;
        List<Double> sortedValue = new ArrayList<>(featSet[i]);
        Collections.sort(sortedValue);
        assert sortedValue.size() < this.param.numSplit;
        for (int j = 0; j < sortedValue.size(); j++) {
          cateFeatVec.set(start + j, sortedValue.get(j));
        }
      }

      sketch.increment(0, cateFeatVec);
      needFlushMatrixSet.add(this.param.sketchName);
    }

    clockAllMatrix(needFlushMatrixSet, true);
  }

  // pull the global sketch from PS, only called once by each worker
  public void getSketch() throws Exception {
    PSModel sketch = model.getPSModel(this.param.sketchName);
    LOG.info("------Get sketch from PS------");
    long startTime = System.currentTimeMillis();
    DenseDoubleVector sketchVector = (DenseDoubleVector) sketch.getRow(0);
    LOG.info(String.format("Get sketch cost: %d ms", System.currentTimeMillis() - startTime));

    for (int i = 0; i < sketchVector.getDimension(); i++) {
      this.sketches[i] = (float) sketchVector.get(i);
    }

    // number of categorical feature
    for (int i = 0; i < cateFeatList.size(); i++) {
      int fid = cateFeatList.get(i);
      int start = fid * this.param.numSplit;
      int splitNum = 1;
      for (int j = 0; j < this.param.numSplit; j++) {
        if (this.sketches[start + j + 1] > this.sketches[start + j]) {
          splitNum++;
        } else
          break;
      }
      this.cateFeatNum.put(fid, splitNum);
    }

    LOG.info("Number of splits of categorical features: " + this.cateFeatNum.entrySet().toString());
    this.phase = GBDTPhase.NEW_TREE;
  }

  // sample feature
  public void sampleFeature() throws Exception {
    LOG.info("------Sample feature------");
    PSModel featSample = model.getPSModel(this.param.sampledFeaturesName);
    Set<String> needFlushMatrixSet = new HashSet<String>(1);

    if (this.param.colSample < 1 && taskContext.getTaskIndex() == 0) {
      long startTime = System.currentTimeMillis();
      // push sampled feature set to the current tree
      if (this.param.colSample < 1) {
        int[] fset = this.trainDataStore.featureMeta.sampleCol(this.param.colSample);
        DenseIntVector sampleFeatureVector = new DenseIntVector(fset.length, fset);
        featSample.increment(currentTree, sampleFeatureVector);
        needFlushMatrixSet.add(this.param.sampledFeaturesName);
      }
      LOG.info(String.format("Sample feature cost: %d ms", System.currentTimeMillis() - startTime));
    }

    clockAllMatrix(needFlushMatrixSet, true);
  }

  // create new tree
  // pull sampled features, initialize tree nodes, reset active nodes, reset instance position,
  // calculate gradient
  public void createNewTree() throws Exception {
    LOG.info("------Create new tree------");
    long startTime = System.currentTimeMillis();
    // 1. create new tree, initialize tree nodes and node stats
    RegTree tree = new RegTree(this.param);
    tree.initTreeNodes();
    this.currentDepth = 1;
    this.forest[this.currentTree] = tree;
    // 2. initialize feature set, if sampled, get from PS, otherwise use all the features
    if (this.param.colSample < 1) {
      // 2.1. pull the sampled features of the current tree
      PSModel featSample = model.getPSModel(this.param.sampledFeaturesName);
      DenseIntVector sampleFeatureVector = (DenseIntVector) featSample.getRow(this.currentTree);
      this.fSet = sampleFeatureVector.getValues();
      this.forest[this.currentTree].fset = sampleFeatureVector.getValues();
    } else {
      // 2.2. if use all the features, only called one
      if (null == this.fSet) {
        this.fSet = new int[this.trainDataStore.featureMeta.numFeature];
        for (int fid = 0; fid < this.fSet.length; fid++) {
          this.fSet[fid] = fid;
        }
      }
    }
    // 3. reset active tree nodes, set all tree nodes to inactive, set thread status to idle
    for (int nid = 0; nid < this.maxNodeNum; nid++) {
      resetActiveTNodes(nid);
    }

    // 4. set root node to active
    addActiveNode(0);

    // 5. reset instance position, set the root node's span
    this.nodePosStart[0] = 0;
    this.nodePosEnd[0] = this.instancePos.length - 1;
    for (int nid = 1; nid < this.maxNodeNum; nid++) {
      this.nodePosStart[nid] = -1;
      this.nodePosEnd[nid] = -1;
    }

    // 6. calculate gradient
    calGradPairs();

    // 7. set phase to run active
    this.phase = GBDTPhase.RUN_ACTIVE;
    LOG.info(String.format("Create new tree cost: %d ms", System.currentTimeMillis() - startTime));
  }

  public void runActiveNode() throws Exception {
    LOG.info("------Run active node------");
    long startTime = System.currentTimeMillis();
    Set<String> needFlushMatrixSet = new HashSet<String>();


    // 1. start threads of active tree nodes
    for (int nid = 0; nid < this.maxNodeNum; nid++) {
      if (this.activeNode[nid] == 1) {
        String histParaName = this.param.gradHistNamePrefix + nid;

        // 1.1. start threads for active nodes to generate histogram
        PSModel histMat = model.getPSModel(histParaName);

        int nodeStart = this.nodePosStart[nid];
        int nodeEnd = this.nodePosEnd[nid];
        int batchSize = this.param.batchNum;
        int batchNum =
          (nodeEnd - nodeStart) / batchSize + (nodeEnd - nodeStart) % batchSize == 0 ? 0 : 1;

        // 1.2. set thread status to batch num
        this.activeNodeStat[nid] = batchNum;

        for (int batch = 0; batch < batchNum; batch++) {
          int start = nodeStart + batch * batchSize;
          int end = nodeStart + (batch + 1) * batchSize;
          if (end > nodeEnd) {
            end = nodeEnd;
          }
          GradHistThread runner = new GradHistThread(this, nid, histMat, start, end);
          this.threadPool.submit(runner);
        }

        // 1.3. set the oplog to active
        int bytesPerItem = this.taskContext.getConf().
          getInt(MLConf.ANGEL_COMPRESS_BYTES(), MLConf.DEFAULT_ANGEL_COMPRESS_BYTES());
        if (!(bytesPerItem >= 1 && bytesPerItem <= 7)) {
          needFlushMatrixSet.add(histParaName);
        }
      }
    }
    // 2. check thread stats, if all threads finish, return
    boolean hasRunning = true;
    while (hasRunning) {
      hasRunning = false;
      for (int nid = 0; nid < this.maxNodeNum; nid++) {
        int stat = this.activeNodeStat[nid];
        if (stat >= 1) {
          hasRunning = true;
          break;
        } else if (stat == -1) {
          LOG.error(String.format("Histogram build thread of tree node[%d] failed", nid));
        }
      }
      if (hasRunning) {
        LOG.debug("current has running thread");
      }
    }
    this.phase = GBDTPhase.FIND_SPLIT;
    LOG.info(String.format("Run active node cost: %d ms", System.currentTimeMillis() - startTime));

    // clock
    clockAllMatrix(needFlushMatrixSet, true);
  }

  // find split
  public void findSplit() throws Exception {
    LOG.info("------Find split------");
    long startTime = System.currentTimeMillis();
    // 1. find responsible tree node, using RR scheme
    List<Integer> responsibleTNode = new ArrayList<>();
    int activeTNodeNum = 0;
    for (int nid = 0; nid < this.activeNode.length; nid++) {
      int isActive = this.activeNode[nid];
      if (isActive == 1) {
        if (this.taskContext.getTaskIndex() == activeTNodeNum) {
          responsibleTNode.add(nid);
        }
        if (++activeTNodeNum >= taskContext.getTotalTaskNum()) {
          activeTNodeNum = 0;
        }
      }
    }
    int[] tNodeId = Maths.intList2Arr(responsibleTNode);
    LOG.info(String
      .format("Task[%d] responsible tree node: %s", this.taskContext.getTaskId().getIndex(),
        responsibleTNode.toString()));

    // 2. pull gradient histogram
    int[] updatedIndices = new int[tNodeId.length]; // the updated indices of the parameter on PS
    int[] updatedSplitFid = new int[tNodeId.length]; // the updated split features
    double[] updatedSplitFvalue = new double[tNodeId.length]; // the updated split value
    double[] updatedSplitGain = new double[tNodeId.length]; // the updated split gain

    boolean isServerSplit = taskContext.getConf()
      .getBoolean(MLConf.ML_GBDT_SERVER_SPLIT(), MLConf.DEFAULT_ML_GBDT_SERVER_SPLIT());
    int splitNum =
      taskContext.getConf().getInt(MLConf.ML_GBDT_SPLIT_NUM(), MLConf.DEFAULT_ML_GBDT_SPLIT_NUM());

    for (int i = 0; i < tNodeId.length; i++) {
      int nid = tNodeId[i];
      LOG.debug(String
        .format("Task[%d] find best split of tree node: %d", this.taskContext.getTaskIndex(), nid));
      // 2.1. get the name of this node's gradient histogram on PS
      String gradHistName = this.param.gradHistNamePrefix + nid;
      // 2.2. pull the histogram
      long pullStartTime = System.currentTimeMillis();
      PSModel histMat = model.getPSModel(gradHistName);
      TIntDoubleVector histogram = null;
      SplitEntry splitEntry = null;
      if (isServerSplit) {
        int matrixId = histMat.getMatrixId();
        GBDTGradHistGetRowFunc func = new GBDTGradHistGetRowFunc(
          new HistAggrParam(matrixId, 0, param.numSplit, param.minChildWeight, param.regAlpha,
            param.regLambda));
        //histogram = (TDoubleVector) ((GetRowResult) histMat.get(func)).getRow();
        splitEntry = ((GBDTGradHistGetRowResult) histMat.get(func)).getSplitEntry();
      } else {
        histogram = (TIntDoubleVector) histMat.getRow(0);
        LOG.debug("Get grad histogram without server split mode, histogram size" + histogram
          .getDimension());
      }
      LOG.info(String
        .format("Pull histogram from PS cost %d ms", System.currentTimeMillis() - pullStartTime));
      GradHistHelper histHelper = new GradHistHelper(this, nid);

      // 2.3. find best split result of this tree node
      if (this.param.isServerSplit) {
        // 2.3.1 using server split
        if (splitEntry.getFid() != -1) {
          int trueSplitFid = this.fSet[splitEntry.getFid()];
          int splitIdx = (int) splitEntry.getFvalue();
          float trueSplitValue = this.sketches[trueSplitFid * this.param.numSplit + splitIdx];
          LOG.info(String.format("Best split of node[%d]: feature[%d], value[%f], "
              + "true feature[%d], true value[%f], losschg[%f]", nid, splitEntry.getFid(),
            splitEntry.getFvalue(), trueSplitFid, trueSplitValue, splitEntry.getLossChg()));
          splitEntry.setFid(trueSplitFid);
          splitEntry.setFvalue(trueSplitValue);
        }

        // update the grad stats of the root node on PS, only called once by leader worker
        if (nid == 0) {
          GradStats rootStats = new GradStats(splitEntry.leftGradStat);
          rootStats.add(splitEntry.rightGradStat);
          this.updateNodeGradStats(nid, rootStats);
        }
        // update the grad stats of children node
        if (splitEntry.fid != -1) {
          // update the left child
          this.updateNodeGradStats(2 * nid + 1, splitEntry.leftGradStat);
          // update the right child
          this.updateNodeGradStats(2 * nid + 2, splitEntry.rightGradStat);
        }

        // 2.3.2 the updated split result (tree node/feature/value/gain) on PS,
        updatedIndices[i] = nid;
        updatedSplitFid[i] = splitEntry.fid;
        updatedSplitFvalue[i] = splitEntry.fvalue;
        updatedSplitGain[i] = splitEntry.lossChg;
      } else {
        // 2.3.3 otherwise, the returned histogram contains the gradient info
        splitEntry = histHelper.findBestSplit(histogram);
        LOG.info(String.format("Best split of node[%d]: feature[%d], value[%f], losschg[%f]", nid,
          splitEntry.getFid(), splitEntry.getFvalue(), splitEntry.getLossChg()));
        // 2.3.4 the updated split result (tree node/feature/value/gain) on PS,
        updatedIndices[i] = nid;
        updatedSplitFid[i] = splitEntry.fid;
        updatedSplitFvalue[i] = splitEntry.fvalue;
        updatedSplitGain[i] = splitEntry.lossChg;
      }
      // 2.3.5 reset this tree node's gradient histogram to 0
      histMat.zero();
    }
    // 3. push split feature to PS
    DenseIntVector splitFeatureVector = new DenseIntVector(this.activeNode.length);
    // 4. push split value to PS
    DenseDoubleVector splitValueVector = new DenseDoubleVector(this.activeNode.length);
    // 5. push split gain to PS
    DenseDoubleVector splitGainVector = new DenseDoubleVector(this.activeNode.length);

    for (int i = 0; i < updatedIndices.length; i++) {
      splitFeatureVector.set(updatedIndices[i], updatedSplitFid[i]);
      splitValueVector.set(updatedIndices[i], updatedSplitFvalue[i]);
      splitGainVector.set(updatedIndices[i], updatedSplitGain[i]);
    }

    PSModel splitFeat = model.getPSModel(this.param.splitFeaturesName);
    splitFeat.increment(this.currentTree, splitFeatureVector);

    PSModel splitValue = model.getPSModel(this.param.splitValuesName);
    splitValue.increment(this.currentTree, splitValueVector);

    PSModel splitGain = model.getPSModel(this.param.splitGainsName);
    splitGain.increment(this.currentTree, splitGainVector);

    // 6. set phase to AFTER_SPLIT
    this.phase = GBDTPhase.AFTER_SPLIT;
    LOG.info(String.format("Find split cost: %d ms", System.currentTimeMillis() - startTime));

    // clock
    Set<String> needFlushMatrixSet = new HashSet<String>(3);
    needFlushMatrixSet.add(this.param.splitFeaturesName);
    needFlushMatrixSet.add(this.param.splitValuesName);
    needFlushMatrixSet.add(this.param.splitGainsName);
    needFlushMatrixSet.add(this.param.nodeGradStatsName);
    clockAllMatrix(needFlushMatrixSet, true);
  }

  public void afterSplit() throws Exception {
    LOG.info("------After split------");
    long startTime = System.currentTimeMillis();
    // 1. get split feature
    PSModel splitFeatModel = model.getPSModel(this.param.splitFeaturesName);
    DenseIntVector splitFeatureVec = (DenseIntVector) splitFeatModel.getRow(currentTree);

    // 2. get split value
    PSModel splitValueModel = model.getPSModel(this.param.splitValuesName);
    DenseDoubleVector splitValueVec = (DenseDoubleVector) splitValueModel.getRow(currentTree);

    // 3. get split gain
    PSModel splitGainModel = model.getPSModel(this.param.splitGainsName);
    DenseDoubleVector splitGainVec = (DenseDoubleVector) splitGainModel.getRow(currentTree);

    // 4. get node weight
    PSModel nodeGradStatsModel = model.getPSModel(this.param.nodeGradStatsName);
    DenseDoubleVector nodeGradStatsVec = (DenseDoubleVector) nodeGradStatsModel.getRow(currentTree);

    LOG.info(
      String.format("Get split result from PS cost %d ms", System.currentTimeMillis() - startTime));

    // 5. split node
    LOG.debug(String.format("Split active node: %s", Arrays.toString(this.activeNode)));
    int[] preActiveNode = this.activeNode.clone();
    for (int nid = 0; nid < this.maxNodeNum; nid++) {
      if (preActiveNode[nid] == 1) {
        this.activeNodeStat[nid] = 1;
        AfterSplitRunner runner =
          new AfterSplitRunner(this, nid, splitFeatureVec, splitValueVec, splitGainVec,
            nodeGradStatsVec);
        this.threadPool.submit(runner);
      }
    }

    // 2. check thread stats, if all threads finish, return
    boolean hasRunning = true;
    while (hasRunning) {
      hasRunning = false;
      for (int nid = 0; nid < this.maxNodeNum; nid++) {
        int stat = this.activeNodeStat[nid];
        if (stat == 1) {
          hasRunning = true;
          break;
        }
      }
      if (hasRunning) {
        LOG.debug("current has running thread");
      }
    }

    LOG.info(String.format("After split cost: %d ms", System.currentTimeMillis() - startTime));

    // 6. clock
    Set<String> needFlushMatrixSet = new HashSet<String>(4);
    needFlushMatrixSet.add(this.param.splitFeaturesName);
    needFlushMatrixSet.add(this.param.splitValuesName);
    needFlushMatrixSet.add(this.param.splitGainsName);
    needFlushMatrixSet.add(this.param.nodeGradStatsName);
    clockAllMatrix(needFlushMatrixSet, true);
  }

  // split the span of one node, reset the instance position
  public void resetInsPos(int nid, int splitFeature, float splitValue) {
    LOG.debug(String
      .format("------Reset instance position of node[%d] split feature[%d] split value[%f]------",
        nid, splitFeature, splitValue));
    int nodePosStart = this.nodePosStart[nid];
    int nodePosEnd = this.nodePosEnd[nid];
    LOG.debug(String.format("Node[%d] instance positions: [%d-%d]", nid, nodePosStart, nodePosEnd));
    int left = nodePosStart;
    int right = nodePosEnd;
    // in case this worker has no instance on this node
    if (left > right) {
      LOG.debug("nodePosStart > nodePosEnd, maybe there is no instance on node:" + nid);
      // set the span of left child
      this.nodePosStart[2 * nid + 1] = left;
      this.nodePosEnd[2 * nid + 1] = right;
      LOG.debug(String.format("Node[%d] instance positions: [%d-%d]", 2 * nid + 1, left, right));
      // set the span of right child
      this.nodePosStart[2 * nid + 2] = left;
      this.nodePosEnd[2 * nid + 2] = right;
      LOG.debug(String.format("Node[%d] instance positions: [%d-%d]", 2 * nid + 2, left, right));
      return;
    }
    while (right > left) {
      // 1. left to right, find the first instance that should be in the right child
      int leftInsIdx = this.instancePos[left];
      float leftValue = (float) this.trainDataStore.instances.get(leftInsIdx).get(splitFeature);
      while (leftValue <= splitValue && left < right) {
        left++;
        leftInsIdx = this.instancePos[left];
        leftValue = (float) this.trainDataStore.instances.get(leftInsIdx).get(splitFeature);
      }
      // 2. right to left, find the first instance that should be in the left child
      int rightInsIdx = this.instancePos[right];
      float rightValue = (float) this.trainDataStore.instances.get(rightInsIdx).get(splitFeature);
      while (rightValue > splitValue && right > left) {
        right--;
        rightInsIdx = this.instancePos[right];
        rightValue = (float) this.trainDataStore.instances.get(rightInsIdx).get(splitFeature);
      }
      // 3. swap two instances
      if (right > left) {
        this.instancePos[left] = rightInsIdx;
        this.instancePos[right] = leftInsIdx;
      }
    }
    // 4. find the cut pos
    int curInsIdx = this.instancePos[left];
    float curValue = (float) this.trainDataStore.instances.get(curInsIdx).get(splitFeature);
    int cutPos = (curValue >= splitValue) ? left : left + 1; // the first instance that is larger
    // than the split value
    // 5. set the span of left child
    this.nodePosStart[2 * nid + 1] = nodePosStart;
    this.nodePosEnd[2 * nid + 1] = cutPos - 1;
    LOG.debug(
      String.format("Node[%d] instance positions: [%d-%d]", 2 * nid + 1, nodePosStart, cutPos - 1));
    // 6. set the span of right child
    this.nodePosStart[2 * nid + 2] = cutPos;
    this.nodePosEnd[2 * nid + 2] = nodePosEnd;
    LOG.debug(
      String.format("Node[%d] instance positions: [%d-%d]", 2 * nid + 2, cutPos, nodePosEnd));
  }

  // set tree node to active
  public void addActiveNode(int nid) {
    this.activeNode[nid] = 1;
    this.activeNodeStat[nid] = 0;
  }

  // set node to leaf
  public void setNodeToLeaf(int nid, float nodeWeight) {
    LOG.debug(String.format("Set node[%d] to leaf node, leaf weight[%f]", nid, nodeWeight));
    this.forest[currentTree].nodes.get(nid).chgToLeaf();
    this.forest[currentTree].nodes.get(nid).setLeafValue(nodeWeight);

  }

  // set node to inactive
  public void resetActiveTNodes(int nid) {
    this.activeNode[nid] = 0;
    this.activeNodeStat[nid] = 0;
  }

  // finish current tree
  public void finishCurrentTree() {
    this.currentTree++;
    this.currentDepth = 1;
  }

  // finish current depth
  public void finishCurrentDepth() {
    this.currentDepth++;
  }

  // set the tree phase
  public void setPhase(int phase) {
    this.phase = phase;
  }

  // check if there is active node
  public boolean hasActiveTNode() {
    LOG.debug(String.format("Check active node: %s", Arrays.toString(activeNode)));
    boolean hasActive = false;
    for (int isActive : this.activeNode) {
      if (isActive == 1) {
        hasActive = true;
        break;
      }
    }
    return hasActive;
  }

  // check if finish all the tree
  public boolean isFinished() {
    LOG.info(String.format("Check if finished, cur tree[%d], max tree[%d]", this.currentTree,
      this.param.treeNum));
    return (this.currentTree >= this.param.treeNum);
  }

  // update node's grad stats on PS
  // called during splitting in GradHistHelper, update the grad stats of children nodes after finding the best split
  // the root node's stats is updated by leader worker
  public void updateNodeGradStats(int nid, GradStats gradStats) throws Exception {
    LOG.debug(String
      .format("Update gradStats of node[%d]: sumGrad[%f], sumHess[%f]", nid, gradStats.sumGrad,
        gradStats.sumHess));
    // 1. create the update
    DenseDoubleVector vec = new DenseDoubleVector(2 * this.activeNode.length);
    vec.set(nid, gradStats.sumGrad);
    vec.set(nid + this.activeNode.length, gradStats.sumHess);
    // 2. push the update to PS
    PSModel nodeGradStats = this.model.getPSModel(this.param.nodeGradStatsName);
    nodeGradStats.increment(this.currentTree, vec);
  }

  public void updateInsPreds() throws Exception {
    LOG.info("------Update instance predictions------");
    long startTime = System.currentTimeMillis();
    int nodeNum = this.forest[currentTree].nodes.size();
    for (int nid = 0; nid < nodeNum; nid++) {
      if (null != this.forest[currentTree].nodes.get(nid) && this.forest[currentTree].nodes.get(nid)
        .isLeaf()) {
        float weight = this.forest[currentTree].nodes.get(nid).getLeafValue();
        LOG.debug(String.format("Leaf weight of node[%d]: %f", nid, weight));
        int nodePosStart = this.nodePosStart[nid];
        int nodePosEnd = this.nodePosEnd[nid];
        for (int i = nodePosStart; i < nodePosEnd; i++) {
          int insIdx = this.instancePos[i];
          this.trainDataStore.preds[insIdx] += this.param.learningRate * weight;
        }
      }
    }

    LOG.info(String
      .format("Update instance predictions cost: %d ms", System.currentTimeMillis() - startTime));
  }

  public void updateLeafPreds() throws Exception {
    LOG.info("------Update leaf node predictions------");
    long startTime = System.currentTimeMillis();
    int nodeNum = this.forest[currentTree].nodes.size();
    DenseDoubleVector vec = new DenseDoubleVector(this.maxNodeNum);
    for (int nid = 0; nid < nodeNum; nid++) {
      if (null != this.forest[currentTree].nodes.get(nid) && this.forest[currentTree].nodes.get(nid)
        .isLeaf()) {
        float weight = this.forest[currentTree].nodes.get(nid).getLeafValue();
        LOG.debug(String.format("Leaf weight of node[%d]: %f", nid, weight));
        vec.set(nid, weight);
      }
    }
    PSModel nodePreds = this.model.getPSModel(this.param.nodePredsName);
    nodePreds.increment(this.currentTree, vec);

    Set<String> needFlushMatrixSet = new HashSet<String>(1);
    if (taskContext.getTaskIndex() == 0) {
      // the leader task adds node prediction to flush list
      needFlushMatrixSet.add(this.param.nodePredsName);
    }
    clockAllMatrix(needFlushMatrixSet, true);

    LOG.info(String
      .format("Update leaf node predictions cost: %d ms", System.currentTimeMillis() - startTime));
  }

  public Tuple1<Double> eval() {
    LOG.info("------Evaluation------");
    long startTime = System.currentTimeMillis();
    EvalMetric evalMetric = this.param.getEvalMetric();
    float error = evalMetric.eval(this.trainDataStore.preds, this.trainDataStore.labels);
    LOG.info(String.format("Error after tree[%d]: %f", this.currentTree, error));
    LOG.info(String.format("Evaluation cost: %d ms", System.currentTimeMillis() - startTime));
    return new Tuple1<>((double) error);
  }

  public Tuple1<Double> predict() {
    LOG.info("------Predict------");
    long startTime = System.currentTimeMillis();
    PSModel splitFeat = this.model.getPSModel(this.param.splitFeaturesName);
    PSModel splitValue = this.model.getPSModel(this.param.splitValuesName);
    PSModel nodePreds = this.model.getPSModel(this.param.nodePredsName);

    TIntVector splitFeatVec = (TIntVector) splitFeat.getRow(this.currentTree);
    TIntDoubleVector splitValueVec = (TIntDoubleVector) splitValue.getRow(this.currentTree);
    TIntDoubleVector nodePredVec = (TIntDoubleVector) nodePreds.getRow(this.currentTree);
    LOG.info(String.format("Prediction of tree[%d]: %s", this.currentTree,
      Arrays.toString(nodePredVec.getValues())));
    for (int insIdx = 0; insIdx < this.validDataStore.numRow; insIdx++) {
      double curPred = treePred(splitFeatVec, splitValueVec, nodePredVec,
        this.validDataStore.instances.get(insIdx));
      this.validDataStore.preds[insIdx] += this.param.learningRate * curPred;
    }

    EvalMetric evalMetric = this.param.getEvalMetric();
    float error = evalMetric.eval(this.validDataStore.preds, this.validDataStore.labels);
    LOG.info(String.format("Error after tree[%d]: %f", this.currentTree, error));
    LOG.info(String.format("Evaluation cost: %d ms", System.currentTimeMillis() - startTime));
    return new Tuple1<>((double) error);
  }

  public double treePred(TIntVector splitFeatVec, TIntDoubleVector splitValueVec,
    TIntDoubleVector nodePredVec, SparseDoubleSortedVector ins) {
    assert splitFeatVec.getDimension() == splitValueVec.getDimension()
      && splitValueVec.getDimension() == nodePredVec.getDimension();
    int nid = 0;
    int splitFeat = splitFeatVec.get(nid);
    double splitValue = splitValueVec.get(nid);
    double pred = nodePredVec.get(nid);

    while (null != this.forest[this.currentTree].nodes.get(nid)
      && !this.forest[this.currentTree].nodes.get(nid).isLeaf() && -1 != splitFeat
      && nid < splitFeatVec.getDimension()) {
      if (ins.get(splitFeat) <= splitValue) {
        nid = 2 * nid + 1;
      } else {
        nid = 2 * nid + 2;
      }
      splitFeat = splitFeatVec.get(nid);
      splitValue = splitValueVec.get(nid);
      pred = nodePredVec.get(nid);
    }

    return pred;
  }

}
