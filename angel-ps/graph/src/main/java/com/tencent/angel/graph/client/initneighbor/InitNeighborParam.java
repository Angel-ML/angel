package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class InitNeighborParam extends UpdateParam {
    private Map<Long, Node> nodeIdToNode;

    public InitNeighborParam(int matrixId, Map<Long, Node> nodeIdToNodes) {
        super(matrixId);
        this.nodeIdToNode = nodeIdToNodes;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        long[] nodeIndices = new long[nodeIdToNode.size()];
        int i = 0;
        for (long nodeId : nodeIdToNode.keySet()) {
            nodeIndices[i++] = nodeId;
        }

        Arrays.sort(nodeIndices);

        List<PartitionUpdateParam> partParams = new ArrayList<>();
        List<PartitionKey> partitions =
                PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

        int nodeIndex = 0;
        int partIndex = 0;
        while (nodeIndex < nodeIndices.length || partIndex < partitions.size()) {
            int length = 0;
            long endOffset = partitions.get(partIndex).getEndCol();
            while (nodeIndex < nodeIndices.length && nodeIndices[nodeIndex] < endOffset) {
                nodeIndex++;
                length++;
            }

            if (length > 0) {
                partParams.add(new PartInitNeighborParam(matrixId,
                        partitions.get(partIndex), nodeIdToNode, nodeIndices, nodeIndex - length, nodeIndex));
            }
            partIndex++;
        }

        return partParams;
    }
}
