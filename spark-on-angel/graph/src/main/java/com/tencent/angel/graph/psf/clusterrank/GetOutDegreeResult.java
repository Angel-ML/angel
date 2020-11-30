package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;

public class GetOutDegreeResult extends GetResult {

    private Long2IntOpenHashMap nodeIdToOutDegree;

    public GetOutDegreeResult(Long2IntOpenHashMap nodeIdToOutDegree) {
        this.nodeIdToOutDegree = nodeIdToOutDegree;
    }

    public Long2IntOpenHashMap getNodeIdToOutDegree() {
        return nodeIdToOutDegree;
    }

}
