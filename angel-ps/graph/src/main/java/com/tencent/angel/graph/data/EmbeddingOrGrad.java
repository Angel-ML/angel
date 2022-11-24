package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EmbeddingOrGrad implements IElement {
    private float[] values;

    public EmbeddingOrGrad(float[] values) {
        this.values = values;
    }

    public EmbeddingOrGrad() {
        this(null);
    }

    @Override
    public Object deepClone() {
        return values;
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeFloats(output, values);
    }

    @Override
    public void deserialize(ByteBuf input) {
        values = ByteBufSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int bufferLen() {
        return ByteBufSerdeUtils.serializedFloatsLen(values);
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeFloats(output, values);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        values = StreamSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

    public float[] getValues() {
        return values;
    }
}
