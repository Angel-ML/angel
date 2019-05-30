package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.client.NodeIDWeightPairs;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborResult extends PartitionGetResult {
    private int partId;
    private NodeIDWeightPairs[] nodeIdsWeights;

    PartSampleNeighborResult(int partId, NodeIDWeightPairs[] nodeIdsWeights) {
        this.partId = partId;
        this.nodeIdsWeights = nodeIdsWeights;
    }

    @Override
    public void serialize(ByteBuf output) {

    }

    @Override
    public void deserialize(ByteBuf input) {

    }

    @Override
    public int bufferLen() {
        return 0;
    }

    public NodeIDWeightPairs[] getNeighborIndices() {
        return nodeIdsWeights;
    }

    public int getPartId() {
        return partId;
    }
}
