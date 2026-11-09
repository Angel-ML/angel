package com.tencent.angel.spark.ml.psf.neighbors.simpleByAliasTable.sample;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 *
 */
public class PartSampleResult extends PartitionGetResult {
    private int partId;
    private long[] nodeIdToSamples;

    public PartSampleResult(int partitionId, long[] nodeIdToSamples) {
        this.partId = partitionId;
        this.nodeIdToSamples = nodeIdToSamples;
        assert (nodeIdToSamples != null);
    }

    public PartSampleResult() {
        this(-1, null);
    }

    public long[] getNodeIdToSamples() {
        return nodeIdToSamples;
    }

    public void setNodeIdToSamples(long[] nodeIdToSamples) {
        this.nodeIdToSamples = nodeIdToSamples;
    }

    public int getPartId() {
        return partId;
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(partId);
        output.writeInt(nodeIdToSamples.length);
        for (int i = 0; i < nodeIdToSamples.length; i++) {
            output.writeLong(nodeIdToSamples[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        partId = input.readInt();
        int size = input.readInt();
        nodeIdToSamples = new long[size];

        for (int i = 0; i < size; i++) {
            nodeIdToSamples[i] = input.readLong();
        }
    }

    @Override
    public int bufferLen() {
        int len = 8;
        len += 8 * nodeIdToSamples.length;
        return len;
    }
}
