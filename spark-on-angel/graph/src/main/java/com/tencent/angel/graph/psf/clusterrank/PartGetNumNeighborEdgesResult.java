package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNumNeighborEdgesResult extends PartitionGetResult {

    private int partId;

    private long[] nodeIdToNumEdges;

    public PartGetNumNeighborEdgesResult(int partId, long[] nodeIdToNumEdges) {
        this.partId = partId;
        this.nodeIdToNumEdges = nodeIdToNumEdges;
    }

    public PartGetNumNeighborEdgesResult() {
        this(-1, null);
    }

    public int getPartId() {
        return partId;
    }

    public long[] getNodeIdToNumEdges() {
        return nodeIdToNumEdges;
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(partId);
        output.writeInt(nodeIdToNumEdges.length);
        for (int i = 0; i < nodeIdToNumEdges.length; i++) {
            output.writeLong(nodeIdToNumEdges[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        partId = input.readInt();
        int size = input.readInt();
        nodeIdToNumEdges = new long[size];
        for (int i = 0; i < size; i++) {
            nodeIdToNumEdges[i] = input.readLong();
        }
    }

    @Override
    public int bufferLen() {
        int len;
        len = 8 + nodeIdToNumEdges.length * 8;
        return len;
    }
}
