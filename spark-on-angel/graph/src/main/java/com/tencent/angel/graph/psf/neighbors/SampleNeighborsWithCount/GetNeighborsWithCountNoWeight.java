package com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount;

import com.tencent.angel.graph.common.psf.result.GetLongsResult;
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

import java.util.List;
import java.util.Random;

public class GetNeighborsWithCountNoWeight extends GetFunc {

    /**
     * Create a new DefaultGetFunc.
     *
     * @param param parameter of get udf
     */
    public GetNeighborsWithCountNoWeight(GetNeighborWithCountParam param) {
        super(param);
    }

    public GetNeighborsWithCountNoWeight() {
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

        // Long type node id
        long[] nodeIds = ((ILongKeyIntValuePartOp) keyValuePart).getKeys();
        int[] count = ((ILongKeyIntValuePartOp) keyValuePart).getValues();
        long[][] neighbors = new long[nodeIds.length][];
        Random r = new Random();

        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];

            // Get node neighbor number
            NeighborsTableElement element = (NeighborsTableElement) (row.get(nodeId));
            if (element == null) {
                neighbors[i] = null;
            } else {
                long[] nodeNeighbors = element.getNeighborIds();
                if (nodeNeighbors == null || nodeNeighbors.length == 0 || count[i] <= 0) {
                    neighbors[i] = null;
                } else {
                    neighbors[i] = new long[count[i]];

                    // start sampling randomly for count times
                    for (int j = 0; j < count[i]; j++) {
                        int index = r.nextInt(nodeNeighbors.length);
                        neighbors[i][j] = nodeNeighbors[index];
                    }
                }
            }
        }

        return new PartGetNeighborWithCountResult(nodeIds, neighbors);

    }


}
