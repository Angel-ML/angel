package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.common.psf.result.GetFloatsResult;
import com.tencent.angel.graph.common.psf.result.PartGetFloatsResult;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetWeights extends GetFunc {

    public GetWeights(LongKeysGetParam param) {
        super(param);
    }

    public GetWeights() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        GeneralPartGetParam getParam = (GeneralPartGetParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, getParam);
        long[] nodeIds = ((ILongKeyPartOp) getParam.getIndicesPart()).getKeys();
        float[][] data = new float[nodeIds.length][];
        for (int i = 0; i < nodeIds.length; i++) {
            DynamicSimpleNeighborElement ele = (DynamicSimpleNeighborElement) row.get(nodeIds[i]);
            data[i] = ele.getWeights();
        }
        return new PartGetFloatsResult(nodeIds, data);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        int resultSize = 0;
        for (PartitionGetResult result : partResults) {
            resultSize += ((PartGetFloatsResult) result).getNodeIds().length;
        }
        Long2ObjectOpenHashMap<float[]> nodeIdToData = new Long2ObjectOpenHashMap<>(resultSize);
        for (PartitionGetResult result : partResults) {
            PartGetFloatsResult partResult = (PartGetFloatsResult) result;
            long[] nodeIds = partResult.getNodeIds();
            float[][] data = partResult.getData();
            for (int i = 0; i < nodeIds.length; i++) {
                nodeIdToData.put(nodeIds[i], data[i]);
            }
        }
        return new GetFloatsResult(nodeIdToData);
    }
}
