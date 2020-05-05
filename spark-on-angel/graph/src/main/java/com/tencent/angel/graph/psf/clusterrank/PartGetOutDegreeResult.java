package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetOutDegreeResult extends PartitionGetResult {

    private int partId;

    private int[] nodeIdToOutDegree;

    public PartGetOutDegreeResult(int partId, int[] nodeIdToOutDegree) {
        this.partId = partId;
        this.nodeIdToOutDegree = nodeIdToOutDegree;
    }

    public PartGetOutDegreeResult() {
        this(-1, null);
    }

    public int getPartId() {
        return partId;
    }

    public int[] getNodeIdToOutDegree() {
        return nodeIdToOutDegree;
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(partId);
        output.writeInt(nodeIdToOutDegree.length);
        for (int i = 0; i < nodeIdToOutDegree.length; i++) {
            output.writeInt(nodeIdToOutDegree[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        partId = input.readInt();
        int size = input.readInt();
        nodeIdToOutDegree = new int[size];
        for (int i = 0; i < size; i++) {
            nodeIdToOutDegree[i] = input.readInt();
        }
    }

    @Override
    public int bufferLen() {
        int len;
        len = 8 + nodeIdToOutDegree.length * 4;
        return len;
    }
}
