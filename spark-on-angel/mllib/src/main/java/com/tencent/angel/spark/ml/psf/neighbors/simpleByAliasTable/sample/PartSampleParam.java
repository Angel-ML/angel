package com.tencent.angel.spark.ml.psf.neighbors.simpleByAliasTable.sample;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleParam extends PartitionGetParam {

    private long[] nodeIds;

    private int startIndex;

    private int endIndex;

    public PartSampleParam(int matrixId, PartitionKey part, long[] nodeIds
            , int startIndex, int endIndex) {
        super(matrixId, part);
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartSampleParam() {
        this(0, null, null, 0, 0);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public void setNodeIds(long[] nodeIds) {
        this.nodeIds = nodeIds;
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
        int num = buf.readInt();
        nodeIds = new long[num];
        for (int i = 0; i < nodeIds.length; i++) {
            nodeIds[i] = buf.readLong();
        }
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + 4 + 8 * (endIndex - startIndex);
    }

}
