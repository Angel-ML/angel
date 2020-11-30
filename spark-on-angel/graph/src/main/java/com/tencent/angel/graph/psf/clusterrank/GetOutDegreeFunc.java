package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

import java.util.List;

public class GetOutDegreeFunc extends GetFunc {

    public GetOutDegreeFunc() {
        this(null);
    }

    public GetOutDegreeFunc(GetParam param) {
        super(param);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartGetOutDegreeParam param = (PartGetOutDegreeParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongIntRow row = (ServerLongIntRow) (((RowBasedPartition) part).getRow(0));
        long[] nodeIds = param.getNodeIds();
        int[] outDegrees = new int[nodeIds.length];

        for (int i = 0; i < nodeIds.length; i++) {
            outDegrees[i] = row.get(nodeIds[i]);
        }

        return new PartGetOutDegreeResult(part.getPartitionKey().getPartitionId(), outDegrees);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
                partResults.size());
        for (PartitionGetResult result : partResults) {
            partIdToResultMap.put(((PartGetOutDegreeResult) result).getPartId(), result);
        }

        GetOutDegreeParam param = (GetOutDegreeParam) getParam();
        long[] nodeIds = param.getNodeIds();
        List<PartitionGetParam> partParams = param.getPartParams();

        Long2IntOpenHashMap nodeIdToDegree = new Long2IntOpenHashMap(nodeIds.length);

        for (PartitionGetParam partParam : partParams) {
            int start = ((PartGetOutDegreeParam) partParam).getStartIndex();
            int end = ((PartGetOutDegreeParam) partParam).getEndIndex();

            PartGetOutDegreeResult partResult = (PartGetOutDegreeResult) (partIdToResultMap
                    .get(partParam.getPartKey().getPartitionId()));
            int[] results = partResult.getNodeIdToOutDegree();
            for (int i = start; i < end; i++) {
                nodeIdToDegree.put(nodeIds[i], results[i - start]);
            }
        }

        return new GetOutDegreeResult(nodeIdToDegree);
    }
}
