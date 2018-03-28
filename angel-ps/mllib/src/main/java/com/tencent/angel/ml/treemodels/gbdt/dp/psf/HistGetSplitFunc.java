package com.tencent.angel.ml.treemodels.gbdt.dp.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.treemodels.gbdt.histogram.SplitFinder;
import com.tencent.angel.ml.treemodels.param.GBDTParam;
import com.tencent.angel.ml.treemodels.tree.basic.SplitEntry;
import com.tencent.angel.ml.treemodels.tree.regression.RegTNodeStat;
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import io.netty.buffer.ByteBuf;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.List;

public class HistGetSplitFunc extends GetFunc {

    public HistGetSplitFunc(int matrixId, int rowId, RegTNodeStat[] nodeStats, GBDTParam param) {
        this(new HistGetSplitParam(matrixId, rowId, param.numClass, nodeStats,
                param.numSplit, param.minChildWeight, param.regAlpha, param.regLambda));
    }

    public HistGetSplitFunc(HistGetSplitParam param) {
        super(param);
    }

    public HistGetSplitFunc() {
        super(null);
    }

    public static class HistGetSplitParam extends GetParam {
        private final int rowId;
        private final int numClass;
        private final RegTNodeStat[] nodeStats;
        private final int numSplit;
        private final float minChildWeight;
        private final float regAlpha;
        private final float regLambda;

        public HistGetSplitParam(int matrixId, int rowId, int numClass, RegTNodeStat[] nodeStats,
                                 int numSplit, float minChildWeight,
                                 float regAlpha, float regLambda) {
            super(matrixId);
            this.rowId = rowId;
            this.numClass = numClass;
            this.nodeStats = nodeStats;
            this.numSplit = numSplit;
            this.minChildWeight = minChildWeight;
            this.regAlpha = regAlpha;
            this.regLambda = regLambda;
        }

        @Override
        public List<PartitionGetParam> split() {
            List<PartitionKey> partList =
                PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

            int size = partList.size();

            List<PartitionGetParam> partparams = new ArrayList<>();
            for (PartitionKey partKey : partList) {
                if (partKey.getStartRow() <= rowId && rowId < partKey.getEndRow()) {
                    partparams.add(new HistGetSplitPartitionParam(matrixId, partKey, rowId,
                            numClass, nodeStats, numSplit, minChildWeight, regAlpha, regLambda));
                }
            }
            return partparams;
        }
    }

    public static class HistGetSplitPartitionParam extends PartitionGetParam {
        private int rowId;
        private int numClass;
        private RegTNodeStat[] nodeStats;
        private int numSplit;
        private float minChildWeight;
        private float regAlpha;
        private float regLambda;

        public HistGetSplitPartitionParam(int matrixId, PartitionKey partKey, int rowId,
                                          int numClass, RegTNodeStat[] nodeStats,
                                          int numSplit, float minChildWeight,
                                          float regAlpha, float regLambda) {
            super(matrixId, partKey);
            this.rowId = rowId;
            this.numClass = numClass;
            this.nodeStats = nodeStats;
            this.numSplit = numSplit;
            this.minChildWeight = minChildWeight;
            this.regAlpha = regAlpha;
            this.regLambda = regLambda;
        }

        public HistGetSplitPartitionParam() {
            this(0, null, 0, 0, null, 0, 0.0f, 0.0f, 0.0f);
        }

        @Override
        public void serialize(ByteBuf buf) {
            super.serialize(buf);
            buf.writeInt(rowId);
            buf.writeInt(numClass);
            for (RegTNodeStat nodeStat : nodeStats) {
                buf.writeFloat(nodeStat.getGain());
                buf.writeFloat(nodeStat.getSumGrad());
                buf.writeFloat(nodeStat.getSumHess());
            }
            buf.writeInt(numSplit);
            buf.writeFloat(minChildWeight);
            buf.writeFloat(regAlpha);
            buf.writeFloat(regLambda);
        }

        @Override
        public void deserialize(ByteBuf buf) {
            super.deserialize(buf);
            rowId = buf.readInt();
            numClass = buf.readInt();
            nodeStats = new RegTNodeStat[numClass == 2 ? 1 : numClass];
            for (int i = 0; i < nodeStats.length; i++) {
                float gain = buf.readFloat();
                float sumGrad = buf.readFloat();
                float sumHess = buf.readFloat();
                nodeStats[i] = new RegTNodeStat(sumGrad, sumHess);
                nodeStats[i].setGain(gain);
            }
            numSplit = buf.readInt();
            minChildWeight = buf.readFloat();
            regAlpha = buf.readFloat();
            regLambda = buf.readFloat();
        }

        @Override
        public int bufferLen() {
            return super.bufferLen() + 24 + 12 * (numClass == 2 ? 1 : numClass);
        }

        public int getRowId() {
            return rowId;
        }

        public int getNumClass() {
            return numClass;
        }

        public RegTNodeStat[] getNodeStats() {
            return nodeStats;
        }

        public int getNumSplit() {
            return numSplit;
        }

        public float getMinChildWeight() {
            return minChildWeight;
        }

        public float getRegAlpha() {
            return regAlpha;
        }

        public float getRegLambda() {
            return regLambda;
        }
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        HistGetSplitPartitionParam param = (HistGetSplitPartitionParam) partParam;
        // 1. get histogram
        ServerDenseFloatRow row = (ServerDenseFloatRow) psContext.getMatrixStorageManager()
            .getRow(param.getMatrixId(), param.getRowId(), param.getPartKey().getPartitionId());

        if (row == null) {
            throw new AngelException("Get null row");
        }
        FloatBuffer histBuf = row.getData();
        // 2. create GBDT param
        GBDTParam gbdtParam = new GBDTParam();
        gbdtParam.numClass = param.getNumClass();
        gbdtParam.numSplit = param.getNumSplit();
        gbdtParam.minChildWeight = param.getMinChildWeight();
        gbdtParam.regAlpha = param.getRegAlpha();
        gbdtParam.regLambda = param.getRegLambda();
        // 3. find best split on current partition
        SplitEntry splitEntry = new SplitEntry();
        int sizePerFeat = param.numSplit * 2 * (param.numClass == 2 ? 1 : param.numClass);
        int startFid = (int) row.getStartCol() / sizePerFeat;
        int endFid = (int) row.getEndCol() / sizePerFeat;
        for (int i = 0, offset = 0; startFid + i < endFid; i++, offset += sizePerFeat) {
            SplitEntry curSplit = SplitFinder.findBestSplitOfOneFeature(
                    startFid + i, histBuf, offset, param.getNodeStats(), gbdtParam);
            splitEntry.update(curSplit);
        }
        // 4. return partition get result
        return new HistGetSplitResult.HistGetSplitPartitionResult(splitEntry);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        SplitEntry splitEntry = new SplitEntry();
        for (PartitionGetResult partResult : partResults) {
            SplitEntry curSplit = ((HistGetSplitResult.HistGetSplitPartitionResult)
                    partResult).getSplitEntry();
            splitEntry.update(curSplit);
        }
        return new HistGetSplitResult(ResponseType.SUCCESS, splitEntry);
    }
}
