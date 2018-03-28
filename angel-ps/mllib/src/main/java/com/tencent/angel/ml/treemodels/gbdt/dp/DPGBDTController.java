package com.tencent.angel.ml.treemodels.gbdt.dp;

import com.tencent.angel.ml.conf.MLConf;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseFloatVector;
import com.tencent.angel.ml.math.vector.SparseIntVector;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.ml.objective.Loss;
import com.tencent.angel.ml.treemodels.gbdt.GBDTController;
import com.tencent.angel.ml.treemodels.gbdt.GBDTModel;
import com.tencent.angel.ml.treemodels.gbdt.GBDTPhase;
import com.tencent.angel.ml.treemodels.gbdt.dp.psf.HistAggrFunc;
import com.tencent.angel.ml.treemodels.gbdt.dp.psf.HistGetSplitFunc;
import com.tencent.angel.ml.treemodels.gbdt.dp.psf.HistGetSplitResult;
import com.tencent.angel.ml.treemodels.gbdt.histogram.Histogram;
import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.storage.DPDataStore;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNode;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNodeStat;
import com.tencent.angel.ml.treemodels.tree.regression.RegTree;
import com.tencent.angel.ml.utils.Maths;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

public class DPGBDTController extends GBDTController<DPDataStore> {
    private static final Log LOG = LogFactory.getLog(DPGBDTController.class);

    private boolean isServerSplit;

    public DPGBDTController(TaskContext taskContext, String parallelMode,
                            GBDTParam param, GBDTModel model,
                            DPDataStore dpDataStore, DPDataStore validDataStore) {
        super(taskContext, parallelMode, param, model, dpDataStore, validDataStore);
    }

    @Override
    public void init() {
        super.init();
        isServerSplit = taskContext.getConf().getBoolean(
                MLConf.ML_GBDT_SERVER_SPLIT(), MLConf.DEFAULT_ML_GBDT_SERVER_SPLIT());
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
            PSModel featSample = model.getPSModel(GBDTModel.FEAT_SAMPLE_MAT());
            if (taskContext.getTaskIndex() == 0) {
                // numFeatSample should be adjusted to number of PS
                int numFeatSample = ((DPGBDTModel) model).numFeatSample();
                int[] findex = new int[param.numFeature];
                Arrays.setAll(findex, i -> i);
                Maths.shuffle(findex);
               fset = Arrays.copyOf(findex, numFeatSample);
                Arrays.sort(fset);
                DenseIntVector featVec = new DenseIntVector(fset.length, fset);
                featSample.increment(currentTree, featVec);
            }
            featSample.clock(true).get();
            if (taskContext.getTaskIndex() != 0) {
                DenseIntVector featVec = (DenseIntVector) featSample.getRow(currentTree);
                fset = featVec.getValues();
            }
        } else if (fset == null) { // create once
            fset = new int[param.numFeature];
            Arrays.setAll(fset, i -> i);
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
            // 2. sum up grad & hess
            updateNodeStat(0, sumGrad, sumHess);
            PSModel nodeStatsModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
            nodeStatsModel.clock(true).get();
            // 3. set root grad stats
            DenseFloatVector nodeStatsVec = (DenseFloatVector) nodeStatsModel.getRow(currentTree);
            sumGrad = nodeStatsVec.get(0);
            sumHess = nodeStatsVec.get(param.maxNodeNum);
            forest[currentTree].getRoot().setGradStats(sumGrad, sumHess);
            LOG.info(String.format("Root sumGrad[%f], sumHess[%f]", sumGrad, sumHess));
        } else {
            float[] sumGrad = new float[param.numClass];
            float[] sumHess = new float[param.numClass];
            //
            // sum up
            //
            // 2. sum up grad & hess
            updateNodeStats(0, sumGrad, sumHess);
            PSModel nodeStatsModel = model.getPSModel(GBDTModel.NODE_GRAD_MAT());
            nodeStatsModel.clock(true).get();
            // 3. set root grad stats
            DenseFloatVector nodeStatsVec = (DenseFloatVector) nodeStatsModel.getRow(currentTree);
            for (int i = 0; i < param.numClass; i++) {
                sumGrad[i] = nodeStatsVec.get(i);
                sumHess[i] = nodeStatsVec.get(param.maxNodeNum * param.numClass + i);
            }
            forest[currentTree].getRoot().setGradStats(sumGrad, sumHess);
            LOG.info(String.format("Root sumGrad%s, sumHess%s",
              Arrays.toString(sumGrad), Arrays.toString(sumHess)));
        }
        LOG.info(String.format("Calc grad pair cost %d ms", System.currentTimeMillis() - startTime));
    }

    @Override
    public void runActiveNodes() throws Exception {
        LOG.info("------Run active node------");
        long buildStart = System.currentTimeMillis();
        // build local histograms
        for (int nid : activeNodes) {
            Histogram hist = histBuilder.buildHistogram(nid);
            histograms.put(nid, hist);
        }
        LOG.info(String.format("Build histograms cost %d ms",
                System.currentTimeMillis() - buildStart));

        // compression update
        Set<String> needFlushMatrices = new HashSet<>(activeNodes.size());
        long aggrStart = System.currentTimeMillis();
        int bytesPerItem = taskContext.getConf().getInt(
                MLConf.ML_COMPRESS_BYTES(), 4);
        if (bytesPerItem < 1 || bytesPerItem > 4 ) {
            LOG.info("Invalid compress configuration: " + bytesPerItem + ", it should be [1, 4].");
            bytesPerItem = 4;
        }
        for (int nid : activeNodes) {
            String histName = GBDTModel.GRAD_HIST_MAT_PREFIX() + nid;
            PSModel histMat = model.getPSModel(histName);
            Histogram hist = histograms.get(nid);
            if (bytesPerItem == 4) {
                histMat.increment(0, hist.flatten());
            } else {
                // TODO: use Histogram in psf directly instead of flatten it
                histMat.update(new HistAggrFunc(histMat.getMatrixId(), false,
                  0, hist.flatten(), bytesPerItem * 8));
            }
            needFlushMatrices.add(histName);
        }
        clockAllMatrix(needFlushMatrices, true);
        if (isServerSplit || bytesPerItem == 4) {
            // if isServerSplit, we must make sure all histogram
            // are updated before servers start to find split;
            // else if using low precision update, we must make sure
            // all histogram are updated before they are accessed
            model.sync();
        }
        LOG.info(String.format("Aggregate histogram cost %d ms",
                System.currentTimeMillis() - aggrStart));

        LOG.info(String.format("Run active nodes cost %d ms",
                System.currentTimeMillis() - buildStart));
        this.phase = GBDTPhase.FIND_SPLIT;
    }

    @Override
    public void findSplit() throws Exception {
        LOG.info("------Find split------");
        long startTime = System.currentTimeMillis();
        // 1. find responsible tree nodes, using RR schema
        List<Integer> responsibleNid = new ArrayList<>();
        int looper = 0;
        for (int nid : activeNodes) {
            if (taskContext.getTaskIndex() == looper) {
                responsibleNid.add(nid);
            }
            if (++looper >= taskContext.getTotalTaskNum()) {
                looper = 0;
            }
        }
        LOG.info(String.format("Worker[%d] responsible tree node(s): %s",
                taskContext.getTaskId().getIndex(), responsibleNid.toString()));
        // 2. allocate
        SparseIntVector splitFidVec = new SparseIntVector(param.maxNodeNum);
        SparseFloatVector splitFvalueVec = new SparseFloatVector(param.maxNodeNum);
        SparseFloatVector splitGainVec = new SparseFloatVector(param.maxNodeNum);
        // 3. find split
        for (int i = 0; i < responsibleNid.size(); i++) {
            int nid = responsibleNid.get(i);
            SplitEntry bestSplit = null;
            PSModel histMat = model.getPSModel(GBDTModel.GRAD_HIST_MAT_PREFIX() + nid);
            if (isServerSplit) {
                // 3.1.a. find best split on PS
                int matrixId = histMat.getMatrixId();
                RegTNode node = forest[currentTree].getNode(nid);
                node.calcGain(param);
                RegTNodeStat[] nodeStats = node.getNodeStats();
                bestSplit = ((HistGetSplitResult) histMat.get(new HistGetSplitFunc(
                        matrixId, 0, nodeStats, param))).getSplitEntry();
                if (bestSplit.getFid() != -1) {
                    int trueSplitFid = fset[bestSplit.getFid()];
                    int splitId = (int) bestSplit.getFvalue();
                    float trueSplitFvalue = trainDataStore.getSplit(trueSplitFid, splitId);
                    bestSplit.setFid(trueSplitFid);
                    bestSplit.setFvalue(trueSplitFvalue);
                }
            } else {
                // 3.1.b. pull histogram and find split
                DenseFloatVector flattenHist = (DenseFloatVector) histMat.getRow(0);
                RegTNode node = forest[currentTree].getNode(nid);
                bestSplit = splitFinder.findBestSplit(flattenHist, node, fset);
            }
            // 3.2. reset this tree node's gradient histogram to 0
            histMat.zero();
            // 3.3. write split info to vectors
            splitFidVec.set(nid, bestSplit.getFid());
            splitFvalueVec.set(nid, bestSplit.getFvalue());
            splitGainVec.set(nid, bestSplit.getLossChg());
            // 3.4. update children grad stats
            if (bestSplit.getFid() != -1) {
                updateNodeStat(2 * nid + 1, bestSplit.getLeftGradPair());
                updateNodeStat(2 * nid + 2, bestSplit.getRightGradPair());
            }
        }
        // 4. push split entry to PS
        Set<String> needFlushMatrices = new HashSet<>(4);
        // 4.1. split feature id
        PSModel splitFidModel = model.getPSModel(GBDTModel.SPLIT_FEAT_MAT());
        splitFidModel.increment(currentTree, splitFidVec);
        needFlushMatrices.add(GBDTModel.SPLIT_FEAT_MAT());
        // 4.2. split feature value
        PSModel splitFvalueModel = model.getPSModel(GBDTModel.SPLIT_VALUE_MAT());
        splitFvalueModel.increment(currentTree, splitFvalueVec);
        needFlushMatrices.add(GBDTModel.SPLIT_VALUE_MAT());
        // 4.3. split loss gain
        PSModel splitGainModel = model.getPSModel(GBDTModel.SPLIT_GAIN_MAT());
        splitGainModel.increment(currentTree, splitGainVec);
        needFlushMatrices.add(GBDTModel.SPLIT_GAIN_MAT());
        // 4.4. node grad stat
        needFlushMatrices.add(GBDTModel.NODE_GRAD_MAT());
        // 4.5. clock
        clockAllMatrix(needFlushMatrices, true);
        // 5. finish current phase
        LOG.info(String.format("Find split cost: %d ms",
          System.currentTimeMillis() - startTime));
        this.phase = GBDTPhase.AFTER_SPLIT;
    }

    @Override
    public void afterSplit() throws Exception {
        LOG.info("------After split------");
        long startTime = System.currentTimeMillis();
        // 1. pull best splits
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
        // 2. pull children grad stats
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
                updateInstancePos(nid, fid, fvalue);
            }
        }
        // 4. set all nodes as inactive
        activeNodes.clear();
        // 5. set phase
        this.phase = GBDTPhase.CHOOSE_ACTIVE;
        LOG.info(String.format("After split cost: %d ms",
                System.currentTimeMillis() - startTime));
    }

    private void updateInstancePos(int nid, int splitFid, float splitFvalue) {
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
            while (left < right) {
                // 1. left to right, find the first instance that should be in the right child
                int leftInsIdx = nodeToIns[left];
                float leftValue = trainDataStore.get(leftInsIdx, splitFid, 0.0f);
                while (left < right && leftValue <= splitFvalue) {
                    left++;
                    leftInsIdx = nodeToIns[left];
                    leftValue = trainDataStore.get(leftInsIdx, splitFid, 0.0f);
                }
                // 2. right to left, find the first instance that should be in the left child
                int rightInsIdx = nodeToIns[right];
                float rightValue = trainDataStore.get(rightInsIdx, splitFid, 0.0f);
                while (left < right && rightValue > splitFvalue) {
                    right--;
                    rightInsIdx = nodeToIns[right];
                    rightValue = trainDataStore.get(rightInsIdx, splitFid, 0.0f);
                }
                // 3. swap two instances
                if (right > left) {
                    nodeToIns[left] = rightInsIdx;
                    nodeToIns[right] = leftInsIdx;
                }
            }
            // 4. find the cut pos
            int curInsId = nodeToIns[left];
            float curValue = trainDataStore.get(curInsId, splitFid, 0.0f);
            int cutPos = (curValue >= splitFvalue) ? left : left + 1;   // the first instance that is larger
                                                                        // than the split value
            // 5. set the span of children
            nodePosStart[2 * nid + 1] = nodeStart;
            nodePosEnd[2 * nid + 1] = cutPos - 1;
            nodePosStart[2 * nid + 2] = cutPos;
            nodePosEnd[2 * nid + 2] = nodeEnd;
            LOG.info(String.format("Left child[%d] span [%d - %d]",
                    2 * nid + 1, nodePosStart[2 * nid + 1], nodePosEnd[2 * nid + 1]));
            LOG.info(String.format("Right child[%d] span [%d - %d]",
                    2 * nid + 2, nodePosStart[2 * nid + 2], nodePosEnd[2 * nid + 2]));
        }
    }

}
