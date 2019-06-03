package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;

import java.util.Map;

public class PartInitNeighborParam extends PartitionUpdateParam {
    private Map<Long, Node> nodeIdToNode;
    private long[] nodeIds;
    private transient int startIndex;
    private transient int endIndex;

    public PartInitNeighborParam(int matrixId, PartitionKey partKey,
                                 Map<Long, Node> nodeIdToNode, long[] nodeIds, int startIndex, int endIndex) {
        super(matrixId, partKey);
        this.nodeIdToNode = nodeIdToNode;
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public Map<Long, Node> getNodeIdToNode() {
        return nodeIdToNode;
    }


}
