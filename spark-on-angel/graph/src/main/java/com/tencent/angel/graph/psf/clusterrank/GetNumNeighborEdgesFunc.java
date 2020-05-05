package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongLongRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

import java.util.List;

public class GetNumNeighborEdgesFunc extends GetFunc {

    public GetNumNeighborEdgesFunc() {
        this(null);
    }

    public GetNumNeighborEdgesFunc(GetParam param) {
        super(param);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartGetNumNeighborEdgesParam param = (PartGetNumNeighborEdgesParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongLongRow row = (ServerLongLongRow) (((RowBasedPartition) part).getRow(0));
        long[] nodeIds = param.getNodeIds();
        long[] numEdges = new long[nodeIds.length];

        for (int i = 0; i < nodeIds.length; i++) {
            numEdges[i] = row.get(nodeIds[i]);
        }

        return new PartGetNumNeighborEdgesResult(part.getPartitionKey().getPartitionId(), numEdges);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {

        Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
                partResults.size());
        for (PartitionGetResult result : partResults) {
            partIdToResultMap.put(((PartGetNumNeighborEdgesResult) result).getPartId(), result);
        }

        GetNumNeighborEdgesParam param = (GetNumNeighborEdgesParam) getParam();
        long[] nodeIds = param.getNodeIds();
        List<PartitionGetParam> partParams = param.getPartParams();

        Long2LongOpenHashMap nodeIdToNumEdges = new Long2LongOpenHashMap(nodeIds.length);

        for (PartitionGetParam partParam : partParams) {
            int start = ((PartGetNumNeighborEdgesParam) partParam).getStartIndex();
            int end = ((PartGetNumNeighborEdgesParam) partParam).getEndIndex();

            PartGetNumNeighborEdgesResult partResult = (PartGetNumNeighborEdgesResult) (partIdToResultMap
                    .get(partParam.getPartKey().getPartitionId()));

            long[] results = partResult.getNodeIdToNumEdges();
            for (int i = start; i < end; i++) {
                nodeIdToNumEdges.put(nodeIds[i], results[i - start]);
            }
        }

        return new GetNumNeighborEdgesResult(nodeIdToNumEdges);
    }
}
