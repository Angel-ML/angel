package com.tencent.angel.ml.treemodels.gbdt.fp;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseFloatVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.ml.objective.Loss;
import com.tencent.angel.ml.treemodels.gbdt.GBDTController;
import com.tencent.angel.ml.treemodels.gbdt.GBDTModel;
import com.tencent.angel.ml.treemodels.gbdt.GBDTPhase;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.RangeBitSetGetRowFunc;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.RangeBitSetGetRowResult;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.RangeBitSetUpdateFunc;
import com.tencent.angel.ml.treemodels.gbdt.histogram.Histogram;
import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.storage.DPDataStore;
import com.tencent.angel.ml.treemodels.storage.FPDataStore;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNode;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNodeStat;
import com.tencent.angel.ml.treemodels.tree.regression.RegTree;
import com.tencent.angel.worker.task.TaskContext;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class FPGBDTController extends GBDTController<FPDataStore> {
    private static final Log LOG = LogFactory.getLog(FPGBDTController.class);

    // map instance to tree node, each item is tree node on which it locates
    private int[] insToNode;

    public FPGBDTController(TaskContext taskContext, String parallelMode,
                            GBDTParam param, GBDTModel model,
                            FPDataStore trainDataStore, DPDataStore validDataStore) {
        super(taskContext, parallelMode, param, model, trainDataStore, validDataStore);
    }

    @Override
    public void init() {
        super.init();
        this.insToNode = new int[trainDataStore.getNumInstances()];
    }

    @Override
    public void createNewTree() throws Exception {
        LOG.info("------Create new tree------");
        long startTime = System.currentTimeMillis();
        // 1. init new tree
        this.forest[currentTree] = new RegTree(param);
        // 2. sample features
        sampleFeature();
        // 3. calc grad pairs
        calGradPairs();
        // 4. clear histogram placeholder
        this.histograms.clear();
        // 5. reset instance position, set the root node's span
        Arrays.setAll(this.nodeToIns, i -> i);
        this.nodePosStart[0] = 0;
        this.nodePosEnd[0] = trainDataStore.getNumInstances() - 1;
        Arrays.setAll(this.insToNode, i -> i);
        // 6. set root as ready
        this.readyNodes.add(0);
        // 7. set phase
        this.phase = GBDTPhase.CHOOSE_ACTIVE;
        LOG.info(String.format("Create new tree cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    @Override
    protected void sampleFeature() throws Exception {
        LOG.info("------Sample feature------");
        long startTime = System.currentTimeMillis();
        if (param.featSampleRatio < 1) {
            IntArrayList featList = new IntArrayList();
            int featLo = trainDataStore.getFeatLo();
            int featHi = trainDataStore.getFeatHi();
            Random random = new Random();
            do {
                for (int fid = featLo; fid < featHi; fid++) {
                    if (random.nextFloat() <= param.featSampleRatio) {
                        featList.add(fid);
                    }
                }
            } while (featList.size() == 0);
            fset = featList.toIntArray();
        } else if (fset == null) {  // create once
            int featLo = trainDataStore.getFeatLo();
            int featHi = trainDataStore.getFeatHi();
            fset = new int[featHi - featLo];
            Arrays.setAll(fset, i -> i + featLo);
        }
        LOG.info(String.format("Sample feature cost %d ms, sample ratio %f, return %d features",
                System.currentTimeMillis() - startTime, param.featSampleRatio, fset.length));
    }

    @Override
    protected void calGradPairs() throws Exception {
        LOG.info("------Calc grad pairs------");
        long startTime = System.currentTimeMillis();
        // calc grad pair, sumGrad and sumHess
        Loss.BinaryLogisticLoss objective = new Loss.BinaryLogisticLoss();
        int numInstances = trainDataStore.getNumInstances();
        float[] labels = trainDataStore.getLabels();
        float[] preds = trainDataStore.getPreds();
        float[] weights = trainDataStore.getWeights();
        //gradPairs = objFunc.calGrad(labels, preds, weights, gradPairs);
        if (param.numClass == 2) {
            float sumGrad = 0.0f;
            float sumHess = 0.0f;
            for (int insId = 0; insId < numInstances; insId++) {
                float prob = objective.transPred(preds[insId]);
                insGrad[insId] = objective.firOrderGrad(prob, labels[insId]) * weights[insId];
                insHess[insId] = objective.secOrderGrad(prob, labels[insId]) * weights[insId];
                sumGrad += insGrad[insId];
                sumHess += insHess[insId];
            }
            // 2. set root grad stats
            forest[currentTree].getRoot().setGradStats(sumGrad, sumHess);
            // 3. leader worker push root grad stats
            if (taskContext.getTaskIndex() == 0) {
                updateNodeStat(0, sumGrad, sumHess);
            }
            LOG.info(String.format("Root sumGrad[%f], sumHess[%f]", sumGrad, sumHess));
        } else {
            float[] sumGrad = new float[param.numClass];
            float[] sumHess = new float[param.numClass];
            //
            // sum up
            //
            // 2. set root grad stats
            forest[currentTree].getRoot().setGradStats(sumGrad, sumHess);
            // 3. leader worker push root grad stats
            if (taskContext.getTaskIndex() == 0) {
                updateNodeStats(0, sumGrad, sumHess);
            }
            LOG.info(String.format("Root sumGrad%s, sumHess%s",
                Arrays.toString(sumGrad), Arrays.toString(sumHess)));
        }
        model.getPSModel(GBDTModel.NODE_GRAD_MAT()).clock(true).get();
        LOG.info(String.format("Calc grad pair cost %d ms", System.currentTimeMillis() - startTime));
    }

    @Override
    public void findSplit() throws Exception {
        LOG.info("------Find split------");
        long startTime = System.currentTimeMillis();
        Set<String> needFlushMatrixSet = new TreeSet<>();
        // 1. find local best splits
        SparseIntVector splitFidVec = new SparseIntVector(param.maxNodeNum);
        SparseFloatVector splitFvalueVec = new SparseFloatVector(param.maxNodeNum);
        SparseFloatVector splitGainVec = new SparseFloatVector(param.maxNodeNum);
        for (int nid : activeNodes) {
            // 1. find local best split of one node
            Histogram histogram = this.histograms.get(nid);
            RegTNode node = this.forest[currentTree].getNode(nid);
            SplitEntry localBest = splitFinder.findBestSplit(histogram, node, fset);
            // 2. write split info to vectors
            splitFidVec.set(nid, localBest.getFid());
            splitFvalueVec.set(nid, localBest.getFvalue());
            splitGainVec.set(nid, localBest.getLossChg());
        }
        // 2. push local best splits to PS
        // 2.1. split feature id
        PSModel splitFidModel = model.getPSModel(GBDTModel.LOCAL_SPLIT_FEAT_MAT());
        splitFidModel.increment(taskContext.getTaskIndex(), splitFidVec);
        // 2.2. split feature value
        PSModel splitFvalueModel = model.getPSModel(GBDTModel.LOCAL_SPLIT_VALUE_MAT());
        splitFvalueModel.increment(taskContext.getTaskIndex(), splitFvalueVec);
        // 2.3. split loss gain
        PSModel splitGainModel = model.getPSModel(GBDTModel.LOCAL_SPLIT_GAIN_MAT());
        splitGainModel.increment(taskContext.getTaskIndex(), splitGainVec);
        // 2.4. clock
        needFlushMatrixSet.add(GBDTModel.LOCAL_SPLIT_FEAT_MAT());
        needFlushMatrixSet.add(GBDTModel.LOCAL_SPLIT_VALUE_MAT());
        needFlushMatrixSet.add(GBDTModel.LOCAL_SPLIT_GAIN_MAT());
        needFlushMatrixSet.add(GBDTModel.NODE_GRAD_MAT());
        clockAllMatrix(needFlushMatrixSet, true);
        // 3. leader worker pull all local best splits, find the global best split
        // do not clear node grad stats
        needFlushMatrixSet.remove(GBDTModel.NODE_GRAD_MAT());
        if (taskContext.getTaskIndex() == 0) {
            LOG.info("Worker[0] find the global best splits");
            // 3.1. pull local best splits
            SparseIntVector[] splitFidMat = new SparseIntVector[param.numWorker];
            SparseFloatVector[] splitFvalueMat = new SparseFloatVector[param.numWorker];
            SparseFloatVector[] splitGainMat = new SparseFloatVector[param.numWorker];
            for (int workerId = 0; workerId < param.numWorker; workerId++) {
                splitFidMat[workerId] = (SparseIntVector) splitFidModel.getRow(workerId);
                splitFvalueMat[workerId] = (SparseFloatVector) splitFvalueModel.getRow(workerId);
                splitGainMat[workerId] = (SparseFloatVector) splitGainModel.getRow(workerId);
            }
            // 3.2. clear local best split matrices for next update
            clearAllMatrices(needFlushMatrixSet, true);
            // 3.3. find global best splits
            splitFidVec.clear();
            splitFvalueVec.clear();
            splitGainVec.clear();
            for (int nid : activeNodes) {
                SplitEntry globalBest = new SplitEntry();
                for (int workerId = 0; workerId < param.numWorker; workerId++) {
                    int fid = splitFidMat[workerId].get(nid);
                    float fvalue = splitFvalueMat[workerId].get(nid);
                    float lossChg = splitGainMat[workerId].get(nid);
                    globalBest.update(lossChg, fid, fvalue);
                }
                splitFidVec.set(nid, globalBest.getFid());
                splitFvalueVec.set(nid, globalBest.getFvalue());
                splitGainVec.set(nid, globalBest.getLossChg());
            }
            // 3.4. push global best splits to PS
            // 3.3.1. split feature id
            splitFidModel = model.getPSModel(GBDTModel.SPLIT_FEAT_MAT());
            splitFidModel.increment(currentTree, splitFidVec);
            // 3.3.2. split feature value
            splitFvalueModel = model.getPSModel(GBDTModel.SPLIT_VALUE_MAT());
            splitFvalueModel.increment(currentTree, splitFvalueVec);
            // 3.3.3. split loss gain
            splitGainModel = model.getPSModel(GBDTModel.SPLIT_GAIN_MAT());
            splitGainModel.increment(currentTree, splitGainVec);
        }
        // 3.3.4. flush & sync
        needFlushMatrixSet.add(GBDTModel.SPLIT_FEAT_MAT());
        needFlushMatrixSet.add(GBDTModel.SPLIT_VALUE_MAT());
        needFlushMatrixSet.add(GBDTModel.SPLIT_GAIN_MAT());
        clockAllMatrix(needFlushMatrixSet, true);
        model.sync();
        // 4. set phase
        this.phase = GBDTPhase.AFTER_SPLIT;
        LOG.info(String.format("Find split cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    @Override
    public void afterSplit() throws Exception {
        LOG.info("------After split------");
        long startTime = System.currentTimeMillis();
        // 1. pull global best splits
        // 1.1. split feature id
        PSModel splitFidModel = model.getPSModel(GBDTModel.SPLIT_FEAT_MAT());
        DenseIntVector splitFidVec = (DenseIntVector) splitFidModel.getRow(currentTree);
        // 1.2. split feature vale
        PSModel splitFvalueModel = model.getPSModel(GBDTModel.SPLIT_VALUE_MAT());
        DenseFloatVector splitFvalueVec = (DenseFloatVector) splitFvalueModel.getRow(currentTree);
        // 1.3. split loss gain
        PSModel splitGainModel = model.getPSModel(GBDTModel.SPLIT_GAIN_MAT());
        DenseFloatVector splitGainVec = (DenseFloatVector) splitGainModel.getRow(currentTree);
        LOG.info(String.format("Get best split entries from PS cost %d ms",
                System.currentTimeMillis() - startTime));
        // 2. reset instance position
        PSModel splitResultModel = model.getPSModel(GBDTModel.SPLIT_RESULT_MAT());
        int matrixId = splitResultModel.getMatrixId();
        for (int nid : activeNodes) {
            // 2.1. get split result for responsible nodes
            int fid = splitFidVec.get(nid);
            float fvalue = splitFvalueVec.get(nid);
            if (trainDataStore.getFeatLo() <= fid && fid < trainDataStore.getFeatHi()) {
                RangeBitSet splitResult = getSplitResult(nid, fid, fvalue);
                splitResultModel.update(new RangeBitSetUpdateFunc(
                        new RangeBitSetUpdateFunc.BitsUpdateParam(
                                matrixId, false, splitResult)));
            }
        }
        // 2.2. clock & sync
        Set<String> needFlushMatrices = new TreeSet<>();
        needFlushMatrices.add(GBDTModel.NODE_GRAD_MAT());
        needFlushMatrices.add(GBDTModel.SPLIT_RESULT_MAT());
        clockAllMatrix(needFlushMatrices, true);
        model.sync();
        // 2.3. pull children grad stats
        PSModel nodeGradModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
        DenseFloatVector nodeGradVec = (DenseFloatVector) nodeGradModel.getRow(currentTree);
        // 3. split nodes
        for (int nid : activeNodes) {
            // 3.1. create split entry
            int fid = splitFidVec.get(nid);
            float fvalue = splitFvalueVec.get(nid);
            float lossChg = splitGainVec.get(nid);
            SplitEntry splitEntry = new SplitEntry(fid, fvalue, lossChg);
            // 3.2. split
            splitNode(nid, splitEntry, nodeGradVec);
            // 3.3. reset instance pos
            if (fid != -1) {
                int nodeStart = nodePosStart[nid];
                int nodeEnd = nodePosEnd[nid];
                RangeBitSet splitResult = ((RangeBitSetGetRowResult) splitResultModel.get(
                        new RangeBitSetGetRowFunc(matrixId, nodeStart, nodeEnd))).getRangeBitSet();
                updateInstancePos(nid, splitResult, fvalue >= 0.0f);
            }
        }
        // 4. set all nodes as inactive
        activeNodes.clear();
        // 5. set phase
        this.phase = GBDTPhase.CHOOSE_ACTIVE;
        LOG.info(String.format("After split cost: %d ms",
                System.currentTimeMillis() - startTime));
    }

    private RangeBitSet getSplitResult(int nid, int splitFid, float splitFvalue) {
        LOG.info(String.format("------Get split result of node[%d]: " +
                        "fid[%d] fvalue[%f]------", nid, splitFid, splitFvalue));
        RegTNode node = forest[currentTree].getNode(nid);
        int nodeStart = nodePosStart[nid];
        int nodeEnd = nodePosEnd[nid];
        RangeBitSet splitResult = new RangeBitSet(nodeStart, nodeEnd);
        LOG.info(String.format("Node[%d] span: [%d-%d]", nid, nodeStart, nodeEnd));
        RegTNodeStat nodeStat = node.getNodeStat();
        if (param.numClass == 2) {
            float leftChildSumGrad = 0;
            float leftChildSumHess = 0;
            float rightChildSumGrad = 0;
            float rightChildSumHess = 0;
            if (nodeStart <= nodeEnd) {
                int[] indices = trainDataStore.getFeatIndices(splitFid);
                int[] bins = trainDataStore.getFeatBins(splitFid);
                float[] splits = trainDataStore.getSplits(splitFid);
                int nnz = indices.length;
                LOG.info(String.format("Candidate splits of feature[%d]: %s",
                        splitFid, Arrays.toString(splits)));
                if (splitFvalue < 0.0f) {
                    // default to right child
                    // pick out instances that should be in left child
                    for (int i = 0; i < nnz; i++) {
                        int insId = indices[i];
                        int binId = bins[i];
                        float insValue = splits[binId];
                        int insPos = insToNode[insId];
                        if (nodeStart <= insPos && insPos <= nodeEnd
                                && insValue < splitFvalue) {
                            splitResult.set(insPos);
                            leftChildSumGrad += insGrad[insId];
                            leftChildSumHess += insHess[insId];
                        }
                    }
                    rightChildSumGrad = nodeStat.getSumGrad() - leftChildSumGrad;
                    rightChildSumHess = nodeStat.getSumHess() - leftChildSumHess;
                } else {
                    // default to left child
                    // pick out instances that should be in right child
                    for (int i = 0; i < nnz; i++) {
                        int insId = indices[i];
                        int binId = bins[i];
                        float insValue = splits[binId];
                        int insPos = insToNode[insId];
                        if (nodeStart <= insPos && insPos <= nodeEnd
                                && insValue >= splitFvalue) {
                            splitResult.set(insPos);
                            rightChildSumGrad += insGrad[insId];
                            rightChildSumHess += insHess[insId];
                        }
                    }
                    leftChildSumGrad = nodeStat.getSumGrad() - rightChildSumGrad;
                    leftChildSumHess = nodeStat.getSumHess() - rightChildSumHess;
                }
                LOG.info(String.format("Node[%d] split: left child with grad stats[%f, %f], "
                                + "right child with grad stats[%f, %f]", nid,
                        leftChildSumGrad, leftChildSumHess, rightChildSumGrad, rightChildSumHess));
            }
            updateNodeStat(2 * nid + 1, leftChildSumGrad, leftChildSumHess);
            updateNodeStat(2 * nid + 2, rightChildSumGrad, rightChildSumHess);
        } else {
            throw new AngelException("Multi-class not implemented yet");
        }
        return splitResult;
    }

    /*
    private void splitNode(int nid, SplitEntry splitEntry, DenseFloatVector nodeGrads) {
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
                float rightSumGrad = nodeGrads.get(right);
                float rightSumHess = nodeGrads.get(right + param.maxNodeNum);
                rightChild.setGradStats(rightSumGrad, rightSumHess);
            } else {
                float[] leftSumGrad = new float[param.numClass];
                float[] leftSumHess = new float[param.numClass];
                for (int i = 0; i < param.numClass; i++) {
                    leftSumGrad[i] = nodeGrads.get(left * param.numClass + i);
                    leftSumHess[i] = nodeGrads.get((left + param.maxNodeNum) * param.numClass + i);
                    leftChild.setGradStats(leftSumGrad, leftSumHess);
                }
                float[] rightSumGrad = new float[param.numClass];
                float[] rightSumHess = new float[param.numClass];
                for (int i = 0; i < param.numClass; i++) {
                    rightSumGrad[i] = nodeGrads.get(right * param.numClass + i);
                    rightSumHess[i] = nodeGrads.get((right + param.maxNodeNum) * param.numClass + i);
                    rightChild.setGradStats(rightSumGrad, rightSumHess);
                }
            }
            // 4. set children as ready
            readyNodes.add(left);
            readyNodes.add(right);
        } else {
            setNodeToLeaf(nid);
        }
    }
    */

    private void updateInstancePos(int nid, RangeBitSet splitResult, boolean defaultLeft) {
        int nodeStart = nodePosStart[nid];
        int nodeEnd = nodePosEnd[nid];
        LOG.info(String.format("------Reset instance position of node[%d] " +
                        "with span [%d-%d]------", nid, nodeStart, nodeEnd));
        // if no instance on this node
        if (nodeStart > nodeEnd) {
            // set the span of left child
            nodePosStart[2 * nid + 1] = nodeStart;
            nodePosEnd[2 * nid + 1] = nodeEnd;
            // set the span of right child
            nodePosStart[2 * nid + 2] = nodeStart;
            nodePosEnd[2 * nid + 2] = nodeEnd;
        } else {
            int left = nodeStart;
            int right = nodeEnd;
            if (defaultLeft) {
                while (left < right) {
                    // left to right, find the first that should be in right child
                    while (left < right && !splitResult.get(left)) {
                        left++;
                    }
                    // right to left, find the first that should be in left child
                    while (left < right && splitResult.get(right)) {
                        right--;
                    }
                    // swap two instances
                    if (left < right) {
                        swapIns(left, right);
                        left++;
                        right--;
                    }
                }
            } else {
                while (left < right) {
                    // left to right, find the first that should be in right child
                    while (left < right && splitResult.get(left)) {
                        left++;
                    }
                    // right to left, find the first that should be in left child
                    while (left < right && !splitResult.get(right)) {
                        right--;
                    }
                    // swap two instances
                    if (left < right) {
                        swapIns(left, right);
                        left++;
                        right--;
                    }
                }
            }
            // find the cut position
            nodePosStart[2 * nid + 1] = nodeStart;
            boolean flag = splitResult.get(left);
            if ((defaultLeft && !flag) || (!defaultLeft && flag)) {
                // left belongs to left child
                nodePosEnd[2 * nid + 1] = left;
                nodePosStart[2 * nid + 2] = left + 1;
            } else {
                // left belongs to right child
                nodePosEnd[2 * nid + 1] = left - 1;
                nodePosStart[2 * nid + 2] = left;
            }
            nodePosEnd[2 * nid + 2] = nodeEnd;

            LOG.info(String.format("Left child[%d] span [%d - %d]",
                    2 * nid + 1, nodePosStart[2 * nid + 1], nodePosEnd[2 * nid + 1]));
            LOG.info(String.format("Right child[%d] span [%d - %d]",
                    2 * nid + 2, nodePosStart[2 * nid + 2], nodePosEnd[2 * nid + 2]));
        }
    }

    private void swapIns(int pos1, int pos2) {
        int ins1 = nodeToIns[pos1];
        int ins2 = nodeToIns[pos2];
        nodeToIns[pos1] = ins2;
        nodeToIns[pos2] = ins1;
        insToNode[ins1] = pos2;
        insToNode[ins2] = pos1;
    }

    public int[] getInsToNode() {
        return insToNode;
    }
}

