package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.graph.client.constent.Constent;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class TypeNeighbors implements IElement {
    private Int2ObjectOpenHashMap<long[]> typeNeighbors;

    public TypeNeighbors(Int2ObjectOpenHashMap<long[]> typeNeighbors) {
        this.typeNeighbors = typeNeighbors;
    }

    public TypeNeighbors() {
        this(null);
    }

    @Override
    public Object deepClone() {
        return typeNeighbors;
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeInt(output, typeNeighbors.size());
        for (Int2ObjectOpenHashMap.Entry<long[]> entry : typeNeighbors
                .int2ObjectEntrySet()) {
            ByteBufSerdeUtils.serializeInt(output, entry.getIntKey());
            long[] neighbors = entry.getValue();
            if (neighbors == null) {
                ByteBufSerdeUtils.serializeLongs(output, Constent.emptyLongs);
            } else {
                ByteBufSerdeUtils.serializeLongs(output, neighbors);
            }
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        int size = ByteBufSerdeUtils.deserializeInt(input);
        typeNeighbors = new Int2ObjectOpenHashMap<>(size);
        for (int i = 0; i < size; i++) {
            int nodeId = ByteBufSerdeUtils.deserializeInt(input);
            long[] neighbors = ByteBufSerdeUtils.deserializeLongs(input);
            typeNeighbors.put(nodeId, neighbors);
        }
    }

    @Override
    public int bufferLen() {
        int len = ByteBufSerdeUtils.INT_LENGTH;
        for (Int2ObjectOpenHashMap.Entry<long[]> entry : typeNeighbors
                .int2ObjectEntrySet()) {
            len += ByteBufSerdeUtils.INT_LENGTH;
            long[] neighbors = entry.getValue();
            if (neighbors == null) {
                len += ByteBufSerdeUtils.serializedLongsLen(Constent.emptyLongs);
            } else {
                len += ByteBufSerdeUtils.serializedLongsLen(neighbors);
            }
        }
        return len;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeInt(output, typeNeighbors.size());
        for (Int2ObjectOpenHashMap.Entry<long[]> entry : typeNeighbors
                .int2ObjectEntrySet()) {
            StreamSerdeUtils.serializeInt(output, entry.getIntKey());
            long[] neighbors = entry.getValue();
            if (neighbors == null) {
                StreamSerdeUtils.serializeLongs(output, Constent.emptyLongs);
            } else {
                StreamSerdeUtils.serializeLongs(output, neighbors);
            }
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        int size = StreamSerdeUtils.deserializeInt(input);
        typeNeighbors = new Int2ObjectOpenHashMap<>(size);
        for (int i = 0; i < size; i++) {
            int nodeId = StreamSerdeUtils.deserializeInt(input);
            long[] neighbors = StreamSerdeUtils.deserializeLongs(input);
            typeNeighbors.put(nodeId, neighbors);
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

    public Int2ObjectOpenHashMap<long[]> getTypeNeighbors() {
        return typeNeighbors;
    }
}

