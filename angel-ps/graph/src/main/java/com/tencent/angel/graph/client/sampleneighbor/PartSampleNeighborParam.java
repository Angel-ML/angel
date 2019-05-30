package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;

public class PartSampleNeighborParam extends PartitionGetParam {
    /**
     * Node ids: it just a view for original node ids
     */
    private long[] nodeIds;

    /**
     * Edge types
     */
    private int[] edgeTypes;

    private int count;

    public PartSampleNeighborParam(int matrixId, PartitionKey part, long[] nodeIds, int[] edgeTypes, int count) {
        super(matrixId, part);
        this.nodeIds = nodeIds;
        this.edgeTypes = edgeTypes;
        this.count = count;
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public int[] getEdgeTypes() {
        return edgeTypes;
    }

    public int getCount() {
        return count;
    }
}
