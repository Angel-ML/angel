package com.tencent.angel.graph.psf.neighbors.DynamicSimple;

import com.tencent.angel.graph.common.psf.result.GetLongsResult;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyIntValuePartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.GetNeighborWithCountParam;
import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.PartGetNeighborWithCountParam;
import com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount.PartGetNeighborWithCountResult;

import java.util.List;
import java.util.Random;

public class SampleWithCount extends GetFunc {

    public SampleWithCount(GetNeighborWithCountParam param) {
        super(param);
    }

    public SampleWithCount() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        return partitionGetWithCount(psContext, partParam);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        int resultSize = 0;
        for (PartitionGetResult result : partResults) {
            resultSize += ((PartGetNeighborWithCountResult) result).getNodeIds().length;
        }
        Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(resultSize);
        for (PartitionGetResult result : partResults) {
            PartGetNeighborWithCountResult getResult = (PartGetNeighborWithCountResult) result;
            long[] nodeIds = getResult.getNodeIds();
            long[][] objs = getResult.getData();
            for (int i = 0; i < nodeIds.length; i++) {
                nodeIdToNeighbors.put(nodeIds[i], objs[i]);
            }
        }
        return new GetLongsResult(nodeIdToNeighbors);
    }

    public static PartitionGetResult partitionGetWithCount(PSContext psContext,
                                                           PartitionGetParam partParam) {
        PartGetNeighborWithCountParam param = (PartGetNeighborWithCountParam) partParam;
        KeyValuePart keyValuePart = param.getIndicesPart();
        long[] nodeIds = ((ILongKeyIntValuePartOp) keyValuePart).getKeys();
        int[] count = ((ILongKeyIntValuePartOp) keyValuePart).getValues();
        long[][] neighbors = new long[nodeIds.length][];
        Random r = new Random();

        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];
            DynamicSimpleNeighborElement element = (DynamicSimpleNeighborElement) (row.get(nodeId));
            if (element == null || count[i] <= 0) {
                neighbors[i] = null;
            } else {
                neighbors[i] = new long[count[i]];
                for (int j = 0; j < count[i]; j++) {
                    neighbors[i][j] = element.sample(r, nodeId);
                }
            }
        }
        return new PartGetNeighborWithCountResult(nodeIds, neighbors);

    }


}