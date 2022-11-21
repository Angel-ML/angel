package com.tencent.angel.ps.storage.vector.element;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class LongIntIntArrayPairElement implements IElement {

    private long[] data;
    private int[] types;
    private int[] indptr;

    public LongIntIntArrayPairElement(long[] data, int[] types, int[] indptr) {
        this.data = data;
        this.types = types;
        this.indptr = indptr;
    }

    public LongIntIntArrayPairElement() {
        this(null, null, null);
    }

    public long[] getData() { return data; }

    public int[] getTypes() { return types; }

    public int[] getIndptr() { return indptr; }

    public void setData(long[] data) {
        this.data = data;
    }

    public void setTypes(int[] types) {
        this.types = types;
    }

    public void setIndptr(int[] indptr) {
        this.indptr = indptr;
    }

    @Override
    public LongIntIntArrayPairElement deepClone() {
        long[] newData = new long[data.length];
        int[] newTypes = new int[types.length];
        int[] newIndptr = new int[indptr.length];
        System.arraycopy(data, 0, newData, 0, data.length);
        System.arraycopy(types, 0, newTypes, 0, types.length);
        System.arraycopy(indptr, 0, newIndptr, 0, indptr.length);
        return new LongIntIntArrayPairElement(newData, newTypes, newIndptr);
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeLongs(output, data);
        ByteBufSerdeUtils.serializeInts(output, types);
        ByteBufSerdeUtils.serializeInts(output, indptr);
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = ByteBufSerdeUtils.deserializeLongs(input);
        types = ByteBufSerdeUtils.deserializeInts(input);
        indptr = ByteBufSerdeUtils.deserializeInts(input);
    }

    @Override
    public int bufferLen() {
        return ByteBufSerdeUtils.serializedLongsLen(data) + ByteBufSerdeUtils.serializedIntsLen(types) + ByteBufSerdeUtils.serializedIntsLen(indptr);
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeLongs(output, data);
        StreamSerdeUtils.serializeInts(output, types);
        StreamSerdeUtils.serializeInts(output, indptr);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        data = StreamSerdeUtils.deserializeLongs(input);
        types = StreamSerdeUtils.deserializeInts(input);
        indptr = StreamSerdeUtils.deserializeInts(input);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
