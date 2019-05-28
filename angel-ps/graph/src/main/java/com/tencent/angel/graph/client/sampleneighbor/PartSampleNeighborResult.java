package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborResult extends PartitionGetResult {
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
}
