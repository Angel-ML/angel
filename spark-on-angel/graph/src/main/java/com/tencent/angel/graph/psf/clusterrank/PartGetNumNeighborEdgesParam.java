package com.tencent.angel.graph.psf.clusterrank;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNumNeighborEdgesParam extends PartitionGetParam {

    private long[] nodeIds;

    private int startIndex;

    private int endIndex;

    public PartGetNumNeighborEdgesParam(int matrixId, PartitionKey partKey,
            long[] nodeIds, int startIndex, int endIndex) {
        super(matrixId, partKey);
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartGetNumNeighborEdgesParam() {
        this(0, null, null, 0, 0);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public int getStartIndex() {
        return startIndex;
    }

    public int getEndIndex() {
        return endIndex;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        buf.writeInt(endIndex - startIndex);
        for (int i = startIndex; i < endIndex; i++) {
            buf.writeLong(nodeIds[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        nodeIds = new long[buf.readInt()];
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = buf.readLong();
        }
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + 4 + 8 * nodeIds.length;
    }

}
