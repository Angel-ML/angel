package com.tencent.angel.ps.storage.vector.element;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class LongIntIntFloatArrayPairElement implements IElement {

    private long[] data;
    private int[] types;
    private int[] indptr;
    private float[] weights;

    public LongIntIntFloatArrayPairElement(long[] data, int[] types, int[] indptr, float[] weights) {
        this.data = data;
        this.types = types;
        this.indptr = indptr;
        this.weights = weights;
    }

    public LongIntIntFloatArrayPairElement() {
        this(null, null, null, null);
    }

    public long[] getData() { return data; }

    public int[] getTypes() { return types; }

    public int[] getIndptr() { return indptr; }

    public float[] getWeights() { return weights; }

    public void setData(long[] data) {
        this.data = data;
    }

    public void setTypes(int[] types) {
        this.types = types;
    }

    public void setIndptr(int[] indptr) {
        this.indptr = indptr;
    }

    public void setWeights(float[] weights) {
        this.weights = weights;
    }

    @Override
    public LongIntIntFloatArrayPairElement deepClone() {
        long[] newData = new long[data.length];
        int[] newTypes = new int[types.length];
        int[] newIndptr = new int[indptr.length];
        float[] newWeights = new float[weights.length];
        System.arraycopy(data, 0, newData, 0, data.length);
        System.arraycopy(types, 0, newTypes, 0, types.length);
        System.arraycopy(indptr, 0, newIndptr, 0, indptr.length);
        System.arraycopy(weights, 0, newWeights, 0, weights.length);
        return new LongIntIntFloatArrayPairElement(newData, newTypes, newIndptr, newWeights);
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeLongs(output, data);
        ByteBufSerdeUtils.serializeInts(output, types);
        ByteBufSerdeUtils.serializeInts(output, indptr);
        ByteBufSerdeUtils.serializeFloats(output, weights);
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = ByteBufSerdeUtils.deserializeLongs(input);
        types = ByteBufSerdeUtils.deserializeInts(input);
        indptr = ByteBufSerdeUtils.deserializeInts(input);
        weights = ByteBufSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int bufferLen() {
        return ByteBufSerdeUtils.serializedLongsLen(data) + ByteBufSerdeUtils.serializedIntsLen(types) +
                ByteBufSerdeUtils.serializedIntsLen(indptr) + ByteBufSerdeUtils.serializedFloatsLen(weights);
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeLongs(output, data);
        StreamSerdeUtils.serializeInts(output, types);
        StreamSerdeUtils.serializeInts(output, indptr);
        StreamSerdeUtils.serializeFloats(output, weights);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        data = StreamSerdeUtils.deserializeLongs(input);
        types = StreamSerdeUtils.deserializeInts(input);
        indptr = StreamSerdeUtils.deserializeInts(input);
        weights = StreamSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
