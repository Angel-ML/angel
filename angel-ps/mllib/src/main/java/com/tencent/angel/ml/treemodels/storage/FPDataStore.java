package com.tencent.angel.ml.treemodels.storage;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.FeatureRowsGetFunc;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.FeatureRowsGetResult;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.FeatureRowsUpdateFunc;
import com.tencent.angel.ml.treemodels.gbdt.fp.psf.FeatureRowsUpdateParam;
import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseDoubleSortedVector;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.ml.treemodels.gbdt.GBDTModel;
import com.tencent.angel.ml.treemodels.sketch.HeapQuantileSketch;
import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.param.TreeParam;
import com.tencent.angel.worker.storage.DataBlock;
import com.tencent.angel.worker.task.TaskContext;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;

/**
 * Feature parallel data store
 */
public class FPDataStore extends DataStore {
    private static final Log LOG = LogFactory.getLog(FPDataStore.class);

    private int[][] featIndices;
    private int[][] featBins;

    private int featLo;
    private int featHi;

    public FPDataStore(TaskContext taskContext, TreeParam param) {
        super(taskContext, param);
        featLo = taskContext.getTaskIndex() * (param.numFeature / param.numWorker);
        featHi = taskContext.getTaskIndex() + 1 == param.numWorker
                ? param.numFeature : featLo + param.numFeature / param.numWorker;
        LOG.info(String.format("Worker[%d] feature range: [%d, %d), %d features in total",
                taskContext.getTaskIndex(), featLo, featHi, param.numFeature));
    }

    @Override
    public void init(DataBlock<LabeledData> dataStorage, final GBDTModel model) throws Exception {
        long initStart = System.currentTimeMillis();

        LOG.info("Create feature parallel data meta, numFeature=" + numFeatures);
        // 1. read data
        Tuple2<IntArrayList, FloatArrayList>[] features =
                readDataAndCreateSketch(dataStorage, model);
        // 2. turn feature values into bin indexes
        findBins(features, nnzLocal, nnzGlobal, model);
        // 3. ensure labels
        ensureLabel(((GBDTParam) param).numClass);


        LOG.info(String.format("Create feature-parallel data meta cost %d ms, numInstance=%d",
                (System.currentTimeMillis() - initStart), numInstances));
    }

    private Tuple2<IntArrayList, FloatArrayList>[] readDataAndCreateSketch(
            DataBlock<LabeledData> dataStorage, final GBDTModel model) throws Exception {
        long readStart = System.currentTimeMillis();
        // 1. read data
        Tuple2<IntArrayList, FloatArrayList>[] features = new Tuple2[numFeatures];
        Arrays.setAll(features, fid -> new Tuple2<>(new IntArrayList(), new FloatArrayList()));
        FloatArrayList labelsList = new FloatArrayList(dataStorage.size());
        nnzLocal = new int[numFeatures];
        numInstances = 0;
        dataStorage.resetReadIndex();
        LabeledData data = dataStorage.read();
        while (data != null) {
            SparseDoubleSortedVector x = (SparseDoubleSortedVector) data.getX();
            int[] indices = x.getIndices();
            double[] values = x.getValues();
            for (int i = 0; i < indices.length; i++) {
                int fid = indices[i];
                float fvalue = (float) values[i];
                features[fid]._1.add(numInstances);
                features[fid]._2.add(fvalue);
            }
            labelsList.add((float) data.getY());
            numInstances++;
            data = dataStorage.read();
        }
        LOG.info(String.format("Worker[%d] has %d instances",
                taskContext.getTaskIndex(), numInstances));
        // 2. push local instance num, sum up feature nnz
        PSModel workerInsModel = model.getPSModel(GBDTModel.INSTANCE_NUM_MAT());
        PSModel nnzModel = model.getPSModel(GBDTModel.NNZ_NUM_MAT());
        if (taskContext.getTaskIndex() == 0) {
            workerInsModel.zero();
            nnzModel.zero();
        }
        model.sync();

        DenseIntVector workerInsVec = new DenseIntVector(param.numWorker);
        workerInsVec.set(taskContext.getTaskIndex(), numInstances);
        workerInsModel.increment(0, workerInsVec);

        Arrays.setAll(nnzLocal, fid -> features[fid]._1.size());
        DenseIntVector nnzVec = new DenseIntVector(numFeatures, nnzLocal);
        nnzModel.increment(0, nnzVec);

        workerInsModel.clock(true).get();
        nnzModel.clock(true).get();

        workerNumIns = ((DenseIntVector) workerInsModel.getRow(0)).getValues();
        nnzGlobal = ((DenseIntVector) nnzModel.getRow(0)).getValues();

        int globalNumIns = 0, insIdOffset = 0;
        for (int workerId = 0; workerId < param.numWorker; workerId++) {
            globalNumIns += workerNumIns[workerId];
            if (workerId < taskContext.getTaskIndex()) {
                insIdOffset += workerNumIns[workerId];
            }
        }
        LOG.info(String.format("Worker[%d] has %d instances, offset: %d, " +
                "instance number of all workers: %s, %d instances in total",
                taskContext.getTaskIndex(), numInstances, insIdOffset,
                Arrays.toString(workerNumIns), globalNumIns));
        // 3. set label for each instance
        DenseFloatVector labelsVec = new DenseFloatVector(globalNumIns);
        for (int insId = 0; insId < numInstances; insId++) {
            int trueId = insId + insIdOffset;
            float label = labelsList.getFloat(insId);
            labelsVec.set(trueId, label);
        }
        PSModel labelsModel = model.getPSModel(GBDTModel.LABEL_MAT());
        labelsModel.increment(0, labelsVec);
        labelsModel.clock(true).get();
        labels = ((DenseFloatVector) labelsModel.getRow(0)).getValues();
        numInstances = globalNumIns;

        // 4. create sketches
        createSketch(features, nnzLocal, nnzGlobal, model);

        LOG.info(String.format("Read data and create sketch cost %d ms",
                System.currentTimeMillis() - readStart));
        return features;
    }

    private void createSketch(Tuple2<IntArrayList, FloatArrayList>[] features,
                              int[] nnzLocal, int[] nnzGlobal, final GBDTModel model) throws Exception {
        long createStart = System.currentTimeMillis();
        // 1. create local quantile sketches
        HeapQuantileSketch[] sketches = new HeapQuantileSketch[numFeatures];
        for (int fid = 0; fid < numFeatures; fid++) {
            sketches[fid] = new HeapQuantileSketch((long) nnzLocal[fid]);
            for (int i = 0; i < nnzLocal[fid]; i++) {
                sketches[fid].update(features[fid]._2.getFloat(i));
            }
        }
        // 2. push to PS and merge on PS
        splits = mergeSketchAndPullQuantiles(sketches, nnzGlobal, model);
        // 3. set zero bin indexes
        zeroBins = new int[numFeatures];
        Arrays.setAll(zeroBins, i -> findZeroBin(splits[i]));

        LOG.info(String.format("Create sketch cost %d ms",
                System.currentTimeMillis() - createStart));
    }

    private void findBins(Tuple2<IntArrayList, FloatArrayList>[] features,
                          int[] nnzLocal, int[] nnzGlobal, final GBDTModel model) throws Exception {
        long startTime = System.currentTimeMillis();

        PSModel featRowModel = model.getPSModel(GBDTModel.FEAT_ROW_MAT());
        int matrixId = featRowModel.getMatrixId();

        int numFeature = param.numFeature;
        int numSplit = param.numSplit;
        int numWorker = param.numWorker;

        int insIdOffset = 0;
        for (int i = 0; i < taskContext.getTaskIndex(); i++) {
            insIdOffset += workerNumIns[i];
        }

        featIndices = new int[featHi - featLo][];
        featBins = new int[featHi - featLo][];

        int fid = 0;
        int batchSize = 1024;
        int[] rowIndexes = new int[batchSize];
        Arrays.setAll(rowIndexes, i -> i);
        while (fid < numFeature) {
            if (fid + batchSize > numFeature) {
                batchSize = numFeature - fid;
                rowIndexes = new int[batchSize];
                Arrays.setAll(rowIndexes, i -> i);
            }
            FeatureRowsUpdateParam<Byte> updateParam = new FeatureRowsUpdateParam<>(
                    matrixId, true, numWorker, taskContext.getTaskIndex(), batchSize, numSplit);
            // 1. set up a batch
            for (int i = 0; i < batchSize; i++) {
                int nnz = nnzLocal[fid + i];
                int[] fIndices = new int[nnz];
                int[] fBins = new int[nnz];
                for (int j = 0; j < nnz; j++) {
                    int trueFid = fid + i;
                    fIndices[j] = features[trueFid]._1.getInt(j) + insIdOffset;
                    fBins[j] = indexOf(features[trueFid]._2.getFloat(j), trueFid);
                }
                updateParam.set(i, fIndices, fBins);
            }
            // 2. push local feature rows to PS
            featRowModel.update(new FeatureRowsUpdateFunc<>(updateParam)).get();
            model.sync();
            // 3. pull global feature rows from PS
            int start = fid, stop = fid + batchSize;
            if (featLo < stop && featHi > start) {
                int from = Math.max(featLo, start) - start;
                int to = Math.min(featHi, stop) - start;
                int[] getRowIndexes;
                if (to - from == batchSize) {
                    getRowIndexes = rowIndexes;
                } else {
                    getRowIndexes = new int[to - from];
                    Arrays.setAll(getRowIndexes, i -> i + from);
                }
                Map<Object, Tuple2<int[], int[]>> featRows =
                        ((FeatureRowsGetResult) featRowModel.get(new FeatureRowsGetFunc<>(
                                matrixId, numWorker, getRowIndexes, numSplit))).getFeatureRows();
                for (Map.Entry<Object, Tuple2<int[], int[]>> entry : featRows.entrySet()) {
                    int rowId = (Integer) entry.getKey();
                    int trueFid = rowId + start;
                    Tuple2<int[], int[]> featRow = entry.getValue();
                    featIndices[trueFid - featLo] = featRow._1;
                    featBins[trueFid - featLo] = featRow._2;
                    if (featIndices[trueFid - featLo].length != nnzGlobal[trueFid]) {
                        throw new AngelException(String.format(
                                "Missing values: feature[%d] has %d but got %d", trueFid,
                                featIndices[trueFid - featLo].length, nnzGlobal[trueFid]));
                    }
                }
            }
            model.sync();
            fid += batchSize;
        }

        LOG.info(String.format("Set feature rows cost %d ms",
                System.currentTimeMillis() - startTime));
    }

    @Override
    public float get(int fid, int insId, float defaultValue) {
        int index = Arrays.binarySearch(featIndices[fid - featLo], insId);
        if (index >= 0) {
            int binId = featBins[fid - featLo][index];
            return splits[fid - featLo][binId];
        } else {
            return defaultValue;
        }
    }

    public int[] getFeatIndices(int fid) {
        return featIndices[fid - featLo];
    }

    public int[] getFeatBins(int fid) {
        return featBins[fid - featLo];
    }

    public int getFeatLo() {
        return featLo;
    }

    public int getFeatHi() {
        return featHi;
    }
}
