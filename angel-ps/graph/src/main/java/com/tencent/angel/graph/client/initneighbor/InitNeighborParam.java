package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.*;

public class InitNeighborParam extends UpdateParam {
    private NodeEdgesPair[] nodeEdgesPairs;

    public InitNeighborParam(int matrixId, NodeEdgesPair[] nodeEdgesPairs) {
        super(matrixId);
        this.nodeEdgesPairs = nodeEdgesPairs;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        Arrays.sort(nodeEdgesPairs);

        List<PartitionUpdateParam> partParams = new ArrayList<>();
        List<PartitionKey> partitions =
                PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

        int nodeIndex = 0;
        int partIndex = 0;
        while (nodeIndex < nodeEdgesPairs.length || partIndex < partitions.size()) {
            int length = 0;
            long endOffset = partitions.get(partIndex).getEndCol();
            while (nodeIndex < nodeEdgesPairs.length && nodeEdgesPairs[nodeIndex].getNode().getId() < endOffset) {
                nodeIndex++;
                length++;
            }

            if (length > 0) {
                NodeEdgesPair[] subNodeEdgesPairs = Arrays.copyOfRange(nodeEdgesPairs, nodeIndex - length, nodeIndex);
                partParams.add(new PartInitNeighborParam(matrixId,
                        partitions.get(partIndex), subNodeEdgesPairs));
            }
            partIndex++;
        }

        return partParams;
    }
}
