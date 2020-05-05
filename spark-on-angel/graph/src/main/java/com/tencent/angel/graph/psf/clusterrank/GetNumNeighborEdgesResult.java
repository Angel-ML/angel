package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class GetNumNeighborEdgesResult extends GetResult {

    private Long2LongOpenHashMap nodeIdToNumEdges;

    public GetNumNeighborEdgesResult(Long2LongOpenHashMap nodeIdToNumEdges) {
        this.nodeIdToNumEdges = nodeIdToNumEdges;
    }

    public Long2LongOpenHashMap getNodeIdToNumEdges() {
        return nodeIdToNumEdges;
    }

}
