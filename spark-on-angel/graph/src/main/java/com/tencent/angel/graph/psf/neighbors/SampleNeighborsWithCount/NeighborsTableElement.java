package com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class NeighborsTableElement implements IElement {

    private long[] neighborIds;

    public NeighborsTableElement() {
        this(null);
    }

    public NeighborsTableElement(long[] neighborIds) {
        this.neighborIds = neighborIds;

    }

    public long[] getNeighborIds() {
        return neighborIds;
    }


    public int getNodesNum() {
        return neighborIds.length;
    }

    public long sample(Random r, long nodeId) {
        if (neighborIds == null || neighborIds.length == 0) return nodeId;
        int index = r.nextInt(neighborIds.length);
        return neighborIds[index];
    }

    @Override
    public Object deepClone() {
        int len = neighborIds.length;
        long[] newNodeIds = new long[len];
        System.arraycopy(neighborIds, 0, newNodeIds, 0, len);

        return new NeighborsTableElement(newNodeIds);
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeLongs(output, neighborIds);
    }

    @Override
    public void deserialize(ByteBuf input) {
        neighborIds = ByteBufSerdeUtils.deserializeLongs(input);
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeLongs(output, neighborIds);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        neighborIds = StreamSerdeUtils.deserializeLongs(input);
    }

    @Override
    public int bufferLen() {
        return ByteBufSerdeUtils.serializedLongsLen(neighborIds);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

}