package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.client.NodeIDWeightPairs;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;

import java.util.Map;

public class SampleNeighborResult extends GetResult {
    /**
     * Node id to neighbors map
     */
    private Map<Long, NodeIDWeightPairs> nodeIdToNeighbors;

    SampleNeighborResult(Map<Long, NodeIDWeightPairs> nodeIdToNeighbors) {
        this.nodeIdToNeighbors = nodeIdToNeighbors;
    }

    public Map<Long, NodeIDWeightPairs> getNodeIdToNeighbors() {
        return nodeIdToNeighbors;
    }

    public void setNodeIdToNeighbors(
            Map<Long, NodeIDWeightPairs> nodeIdToNeighbors) {
        this.nodeIdToNeighbors = nodeIdToNeighbors;
    }
}
