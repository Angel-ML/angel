package com.tencent.angel.spark.ml.ann_core.psf.getVectors;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import com.tencent.angel.spark.ml.ann_core.ANNVector;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetVectors extends GetFunc {
    private int matrixId;
    public GetVectors(GetVectorsParam param) {
        super(param);
        matrixId = param.getMatrixId();
    }

    public GetVectors(int matrixId, long[] nodes) {
        super(new GetVectorsParam(matrixId, nodes));
    }

    public GetVectors() {
        super(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        GeneralPartGetParam param = (GeneralPartGetParam) partParam;
        KeyPart keyPart = param.getIndicesPart();
        long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        Long2ObjectOpenHashMap<float[]> vec = new Long2ObjectOpenHashMap<>(nodeIds.length);

        for (int i = 0; i < nodeIds.length; i += 1) {
            if (row.exist(nodeIds[i])) {
                ANNVector tv = (ANNVector)(row.get(nodeIds[i]));
                vec.put(nodeIds[i], tv.getVector());
            }
        }

        return new GetVectorsPartResult(vec);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        int size = 0;
        for (PartitionGetResult result: partResults) {
            size += ((GetVectorsPartResult) result).size();
        }

        Long2ObjectOpenHashMap<float[]> vector = new Long2ObjectOpenHashMap<>(size);

        for (PartitionGetResult result: partResults) {
            vector.putAll(((GetVectorsPartResult) result).getVec());
        }
        return new GetVectorsResult(vector);
    }


}
