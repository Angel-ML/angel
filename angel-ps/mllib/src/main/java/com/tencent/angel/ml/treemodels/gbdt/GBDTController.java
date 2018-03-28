package com.tencent.angel.ml.treemodels.gbdt;

import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.metric.EvalMetric;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.ml.objective.ObjFunc;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.ClearUpdate;
import com.tencent.angel.ml.treemodels.gbdt.histogram.Histogram;
import com.tencent.angel.ml.treemodels.gbdt.histogram.HistogramBuilder;
import com.tencent.angel.ml.treemodels.gbdt.histogram.SplitFinder;
import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.storage.DPDataStore;
import com.tencent.angel.ml.treemodels.storage.DataStore;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.regression.GradPair;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNode;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNodeStat;
import com.tencent.angel.ml.treemodels.tree.regression.RegTree;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;
import java.util.concurrent.Future;

public abstract class GBDTController<TrainDataStore extends DataStore> {
    protected static final Log LOG = LogFactory.getLog(GBDTController.class);

    protected final TaskContext taskContext;
    protected final String parallelMode;
    protected final GBDTParam param;
    protected final GBDTModel model;

    protected final TrainDataStore trainDataStore;
    protected final DPDataStore validDataStore;

    protected RegTree[] forest;  // all regression trees
    protected int currentTree;  // current tree id
    protected GBDTPhase phase;  // current phase

    protected ObjFunc objFunc;  // objective function
    protected float[] insGrad;  // first-order gradient of training instances
    protected float[] insHess;  // second-order gradient of training instances

    protected int[] fset;  // subsample features
    protected Set<Integer> readyNodes;  // nodes ready to process
    protected Set<Integer> activeNodes;  // nodes in process

    protected Map<Integer, Histogram> histograms;  // local histograms, saved for subtraction
    protected HistogramBuilder histBuilder;  // builder in charge of multi-thread building
    protected SplitFinder splitFinder;  // finder in charge of finding best splits

    // map tree node to instance, each item is an instance id
    protected int[] nodeToIns;
    protected int[] nodePosStart;
    protected int[] nodePosEnd;

    public GBDTController(TaskContext taskContext, String parallelMode,
                          GBDTParam param, GBDTModel model,
                          TrainDataStore trainDataStore, DPDataStore validDataStore) {
        this.taskContext = taskContext;
        this.parallelMode = parallelMode;
        this.param = param;
        this.model = model;
        this.trainDataStore = trainDataStore;
        this.validDataStore = validDataStore;
    }

    public void init() {
        forest = new RegTree[param.numTree];
        currentTree = 0;
        phase = GBDTPhase.NEW_TREE;
        //objFunc = new RegLossObj(new Loss.BinaryLogisticLoss());
        insGrad = new float[trainDataStore.getNumInstances()];
        insHess = new float[trainDataStore.getNumInstances()];
        // nodes should be sorted so that the orders
        // are the same when all workers iterate them
        readyNodes = new TreeSet<>();
        activeNodes = new TreeSet<>();
        histograms = new TreeMap<>();
        histBuilder = new HistogramBuilder(param, model, this);
        splitFinder = new SplitFinder(param, this, trainDataStore);
        nodeToIns = new int[trainDataStore.getNumInstances()];
        nodePosStart = new int[param.maxNodeNum];
        nodePosEnd = new int[param.maxNodeNum];
        nodePosEnd[0] = trainDataStore.getNumInstances() - 1;
        // predictions and weights for instances
        int numTrain = trainDataStore.getNumInstances();
        int numValid = validDataStore.getNumInstances();
        int size = param.numClass == 2 ? 1 : param.numClass;
        float[] trainPreds = new float[numTrain * size];
        float[] trainWeights = new float[numTrain * size];
        Arrays.fill(trainWeights, 1.0f);
        trainDataStore.setPreds(trainPreds);
        trainDataStore.setWeights(trainWeights);
        float[] validPreds = new float[numValid * size];
        float[] validWeights = new float[numValid * size];
        Arrays.fill(validWeights, 1.0f);
        validDataStore.setPreds(validPreds);
        validDataStore.setWeights(validWeights);
    }

    protected void clockAllMatrix(Set<String> needFlushMatrices, boolean wait) throws Exception {
        long startTime = System.currentTimeMillis();

        List<Future> clockFutures = new ArrayList<Future>(needFlushMatrices.size());
        for (Map.Entry<String, PSModel> entry : model.getPSModels().entrySet()) {
            if (needFlushMatrices.contains(entry.getKey())) {
                clockFutures.add(entry.getValue().clock(true));
            } else {
                clockFutures.add(entry.getValue().clock(false));
            }
        }

        if (wait) {
            for (Future future : clockFutures) {
                future.get();
            }
        }

        LOG.info(String.format("clock and flush matrices %s cost %d ms",
                needFlushMatrices, System.currentTimeMillis() - startTime));
    }

    protected void clearAllMatrices(Set<String> needClearMatrices, boolean wait) throws Exception {
        List<Future> clockFutures = new ArrayList<Future>(needClearMatrices.size());
        for (Map.Entry<String, PSModel> entry : model.getPSModels().entrySet()) {
            if (needClearMatrices.contains(entry.getKey())) {
                int matrixId = entry.getValue().getMatrixId();
                ClearUpdate clearUpdate = new ClearUpdate(new ClearUpdate.ClearUpdateParam(matrixId, false));
                clockFutures.add(entry.getValue().update(clearUpdate));
            }
        }

        if (wait) {
            for (Future future : clockFutures) {
                future.get();
            }
        }
    }

    public abstract void createNewTree() throws Exception;

    protected abstract void sampleFeature() throws Exception;

    protected abstract void calGradPairs() throws Exception;

    public void chooseActive() {
        LOG.info("------Choose active nodes------");
        Integer[] readyNodesArr = new Integer[readyNodes.size()];
        readyNodes.toArray(readyNodesArr);
        LOG.info("Ready nodes: " + Arrays.toString(readyNodesArr));
        float maxGain = Float.MIN_VALUE;
        int maxGainNid = -1;
        boolean leafWise = false;
        for (Integer nid : readyNodesArr) {
            if (nid == 0) {
                // activate root node
                readyNodes.remove(nid);
                activeNodes.add(nid);
                break;
            }
            if (2 * nid + 1 >= param.maxNodeNum) {
                readyNodes.remove(nid);
                setNodeToLeaf(nid);
            } else {
                RegTNodeStat[] nodeStats = forest[currentTree].getNode(nid).getNodeStats();
                float maxWeight = Float.MIN_VALUE;
                for (RegTNodeStat nodeStat : nodeStats) {
                    maxWeight = Math.max(maxWeight, nodeStat.getSumHess());
                }
                if (maxWeight < param.minChildWeight * 2) {
                    readyNodes.remove(nid);
                    setNodeToLeaf(nid);
                } else {
                    if (leafWise) {
                        // leaf-wise
                        RegTNode node = forest[currentTree].getNode(nid);
                        float[] gains = node.calcGain(param);
                        for (float gain : gains) {
                            if (gain > maxGain) {
                                maxGain = gain;
                                maxGainNid = nid;
                            }
                        }
                    } else {
                        // level-wise
                        readyNodes.remove(nid);
                        activeNodes.add(nid);
                    }
                }
            }
        }
        if (leafWise && maxGainNid != -1) {
            activeNodes.add(maxGainNid);
            readyNodes.remove(maxGainNid);
        }
        if (activeNodes.size() > 0) {
            LOG.info("Active nodes: " + activeNodes.toString());
            this.phase = GBDTPhase.RUN_ACTIVE;
        } else {
            LOG.info("No active nodes");
            for (int nid : readyNodes) {
                setNodeToLeaf(nid);
            }
            readyNodes.clear();
            this.phase = GBDTPhase.FINISH_TREE;
        }
    }

    public void runActiveNodes() throws Exception {
        LOG.info("------Run active node------");
        long startTime = System.currentTimeMillis();
        for (int nid : activeNodes) {
            Histogram hist = histBuilder.buildHistogram(nid);
            histograms.put(nid, hist);
        }
        LOG.info(String.format("Build histograms cost %d ms",
                System.currentTimeMillis() - startTime));
        this.phase = GBDTPhase.FIND_SPLIT;
    }

    public abstract void findSplit() throws Exception;

    public abstract void afterSplit() throws Exception;

    protected void splitNode(int nid, SplitEntry splitEntry, DenseFloatVector nodeGrads) {
        LOG.info(String.format("Split node[%d]: feature[%d], value[%f], lossChg[%f]",
                nid, splitEntry.getFid(), splitEntry.getFvalue(), splitEntry.getLossChg()));
        // 1. set split info to this node
        RegTNode node = forest[currentTree].getNode(nid);
        node.setSplitEntry(splitEntry);
        if (splitEntry.getFid() != -1) {
            // 2. set children nodes of this node
            int left = 2 * nid + 1;
            int right = 2 * nid + 2;
            RegTNode leftChild = new RegTNode(left, node, param.numClass);
            RegTNode rightChild = new RegTNode(right, node, param.numClass);
            node.setLeftChild(leftChild);
            node.setRightChild(rightChild);
            forest[currentTree].setNode(left, leftChild);
            forest[currentTree].setNode(right, rightChild);
            // 3. set grad stats for children
            if (param.numClass == 2) {
                float leftSumGrad = nodeGrads.get(left);
                float leftSumHess = nodeGrads.get(left + param.maxNodeNum);
                leftChild.setGradStats(leftSumGrad, leftSumHess);
                LOG.info(String.format("Left child[%d] sumGrad[%f] sumHess[%f]",
                  2 * nid + 1, leftSumGrad, leftSumHess));
                float rightSumGrad = nodeGrads.get(right);
                float rightSumHess = nodeGrads.get(right + param.maxNodeNum);
                rightChild.setGradStats(rightSumGrad, rightSumHess);
                LOG.info(String.format("Right child[%d] sumGrad[%f] sumHess[%f]",
                  2 * nid + 2, rightSumGrad, rightSumHess));
            } else {
                float[] leftSumGrad = new float[param.numClass];
                float[] leftSumHess = new float[param.numClass];
                for (int i = 0; i < param.numClass; i++) {
                    leftSumGrad[i] = nodeGrads.get(left * param.numClass + i);
                    leftSumHess[i] = nodeGrads.get((left + param.maxNodeNum) * param.numClass + i);
                    leftChild.setGradStats(leftSumGrad, leftSumHess);
                }
                LOG.info(String.format("Left child[%d] sumGrad%s sumHess%s",
                  2 * nid + 1, Arrays.toString(leftSumGrad), Arrays.toString(leftSumHess)));
                float[] rightSumGrad = new float[param.numClass];
                float[] rightSumHess = new float[param.numClass];
                for (int i = 0; i < param.numClass; i++) {
                    rightSumGrad[i] = nodeGrads.get(right * param.numClass + i);
                    rightSumHess[i] = nodeGrads.get((right + param.maxNodeNum) * param.numClass + i);
                    rightChild.setGradStats(rightSumGrad, rightSumHess);
                }
                LOG.info(String.format("Right child[%d] sumGrad%s sumHess%s",
                  2 * nid + 1, Arrays.toString(rightSumGrad), Arrays.toString(rightSumHess)));
            }
            // 4. set children as ready
            readyNodes.add(left);
            readyNodes.add(right);
        } else {
            setNodeToLeaf(nid);
        }
    }

    public void finishCurrentTree(EvalMetric[] evalMetrics) throws Exception {
        updateLeafPreds();
        evaluate(evalMetrics);
        this.currentTree++;
        if (isFinished()) {
            this.phase = GBDTPhase.FINISHED;
        }
        else {
            this.phase = GBDTPhase.NEW_TREE;
        }
    }

    private void evaluate(EvalMetric[] evalMetrics) {
        LOG.info("------Evaluation------");
        long startTime = System.currentTimeMillis();
        updateInsPreds();
        for (EvalMetric metric : evalMetrics) {
            float trainMetric = metric.eval(trainDataStore.getPreds(), trainDataStore.getLabels());
            float validMetric = metric.eval(validDataStore.getPreds(), validDataStore.getLabels());
            LOG.info(String.format("Train %s after tree[%d]: %f",
                    metric.getName(), currentTree, trainMetric));
            LOG.info(String.format("Valid %s after tree[%d]: %f",
                    metric.getName(), currentTree, validMetric));
        }
        LOG.info(String.format("Evaluation cost %d ms", System.currentTimeMillis() - startTime));
    }

    private void updateInsPreds() {
        LOG.info("------Update instance predictions------");
        long startTime = System.currentTimeMillis();
        // 1. update training instance predictions
        for (int nid = 0; nid < param.maxNodeNum; nid++) {
            RegTNode node = forest[currentTree].getNode(nid);
            if (node != null && node.isLeaf()) {
                int nodeStart = nodePosStart[nid];
                int nodeEnd = nodePosEnd[nid];
                if (param.numClass == 2) {
                    RegTNodeStat nodeStat = node.getNodeStat();
                    float leafWeight = nodeStat.getNodeWeight();
                    //LOG.info(String.format("Leaf[%d] weight: %f, span: [%d-%d]",
                    //        nid, leafWeight, nodeStart, nodeEnd));
                    for (int pos = nodeStart; pos < nodeEnd; pos++) {
                        int insId = nodeToIns[pos];
                        float oriPred = trainDataStore.getPred(insId);
                        float newPred = oriPred + param.learningRate * leafWeight;
                        trainDataStore.setPred(insId, newPred);
                    }
                } else {
                    RegTNodeStat[] nodeStats = node.getNodeStats();
                    float[] leafWeights = new float[param.numClass];
                    for (int i = 0; i < param.numClass; i++) {
                        leafWeights[i] = nodeStats[i].getNodeWeight();
                    }
                    //LOG.info(String.format("Leaf[%d] weight: %s, span: [%d-%d]",
                    //        nid, Arrays.toString(leafWeights), nodeStart, nodeEnd));
                    for (int pos = nodeStart; pos < nodeEnd; pos++) {
                        int insId = nodeToIns[pos];
                        for (int i = 0; i < param.numClass; i++) {
                            float oriPred = trainDataStore.getPred(insId * param.numClass + i);
                            float newPred = oriPred + param.learningRate * leafWeights[i];
                            trainDataStore.setPred(insId * param.numClass + i, newPred);
                        }
                    }
                }
            }
        }
        // 2. update validate instance predictions
        validDataStore.additiveUpdatePreds(forest[currentTree], param);
        LOG.info(String.format("Update instance predictions cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    protected void updateLeafPreds() throws Exception {
        LOG.info("------Update leaf node predictions------");
        long startTime = System.currentTimeMillis();
        PSModel nodePredsModel = model.getPSModel(GBDTModel.NODE_PRED_MAT());
        DenseFloatVector nodePredsVec;
        if (param.numClass == 2) {
            nodePredsVec = new DenseFloatVector(param.maxNodeNum);
            for (int nid = 0; nid < param.maxNodeNum; nid++) {
                RegTNode node = forest[currentTree].getNode(nid);
                if (node != null && node.isLeaf()) {
                    float weight = node.getNodeStat().getNodeWeight();
                    nodePredsVec.set(nid, weight);
                    LOG.info(String.format("Leaf[%d] weight: %f",
                            nid, weight));
                }
            }
        } else {
            nodePredsVec = new DenseFloatVector(param.maxNodeNum * param.numClass);
            for (int nid = 0; nid < param.maxNodeNum; nid++) {
                RegTNode node = forest[currentTree].getNode(nid);
                if (node != null && node.isLeaf()) {
                    float[] weights = new float[param.numClass];
                    for (int i = 0; i < param.numClass; i++) {
                        weights[i] = node.getNodeStat(i).getNodeWeight();
                        nodePredsVec.set(nid * param.numClass + i, weights[i]);
                    }
                    LOG.info(String.format("Leaf[%d] weights: %s",
                            nid, Arrays.toString(weights)));
                }
            }
        }
        if (taskContext.getTaskIndex() == 0) {
            nodePredsModel.increment(currentTree, nodePredsVec);
        }
        nodePredsModel.clock(true).get();
        LOG.info(String.format("Update leaf node predictions cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    public boolean isFinished() {
        return currentTree >= param.numTree;
    }

    protected void setNodeToLeaf(int nid) {
        forest[currentTree].getNode(nid).chgToLeaf();
        forest[currentTree].getNode(nid).calcWeight(param);
    }

    protected void updateNodeStat(int nid, GradPair gradPair) {
        DenseFloatVector vec = new DenseFloatVector(param.maxNodeNum * 2);
        vec.set(nid, gradPair.getGrad());
        vec.set(nid + param.maxNodeNum, gradPair.getHess());
        PSModel nodeStatsModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
        nodeStatsModel.increment(currentTree, vec);
    }

    protected void updateNodeStats(int nid, GradPair[] gradPair) {
        DenseFloatVector vec = new DenseFloatVector(param.numClass * param.maxNodeNum * 2);
        for (int i = 0; i < param.numClass; i++) {
            vec.set(nid * param.numClass + i, gradPair[i].getGrad());
            vec.set((nid + param.maxNodeNum) * param.numClass + i, gradPair[i].getHess());
        }
        PSModel nodeStatsModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
        nodeStatsModel.increment(currentTree, vec);
    }

    protected void updateNodeStat(int nid, float sumGrad, float sumHess) {
        DenseFloatVector vec = new DenseFloatVector(param.maxNodeNum * 2);
        vec.set(nid, sumGrad);
        vec.set(nid + param.maxNodeNum, sumHess);
        PSModel nodeStatsModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
        nodeStatsModel.increment(currentTree, vec);
    }

    protected void updateNodeStats(int nid, float[] sumGrad, float[] sumHess) {
        DenseFloatVector vec = new DenseFloatVector(param.numClass * param.maxNodeNum * 2);
        for (int i = 0; i < param.numClass; i++) {
            vec.set(nid * param.numClass + i, sumGrad[i]);
            vec.set((nid + param.maxNodeNum) * param.numClass + i, sumHess[i]);
        }
        PSModel nodeStatsModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
        nodeStatsModel.increment(currentTree, vec);
    }

    public final String getParallelMode() {
        return parallelMode;
    }

    public final TrainDataStore getTrainDataStore() {
        return trainDataStore;
    }

    public final DPDataStore getValidDataStore() {
        return validDataStore;
    }

    public int[] getFset() {
        return fset;
    }

    public GBDTPhase getPhase() {
        return phase;
    }

    public RegTree getTree(int treeId) {
        return forest[treeId];
    }

    public RegTree getLastTree() {
        return isFinished() ? forest[currentTree - 1] : forest[currentTree];
    }

    public Histogram getHistogram(int nid) {
        return histograms.get(nid);
    }

    public float[] getInsGrad() {
        return insGrad;
    }

    public float[] getInsHess() {
        return insHess;
    }

    public int getNodePosStart(int nid) {
        return nodePosStart[nid];
    }

    public int getNodePosEnd(int nid) {
        return nodePosEnd[nid];
    }

    public int[] getNodeToIns() {
        return nodeToIns;
    }
}
