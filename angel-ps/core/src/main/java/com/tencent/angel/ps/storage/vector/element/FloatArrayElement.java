package com.tencent.angel.ps.storage.vector.element;

import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FloatArrayElement implements IElement {

    private float[] data;

    public FloatArrayElement(float[] data) {
        this.data = data;
    }

    public float[] getData() {
        return data;
    }

    public void setData(float[] data) {
        this.data = data;
    }

    @Override
    public FloatArrayElement deepClone() {
        float[] newData = new float[data.length];
        System.arraycopy(data, 0, newData, 0, data.length);
        return new FloatArrayElement(newData);
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeFloat(data.length);
        for (int i = 0; i < data.length; i++) {
            output.writeFloat(data[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = new float[input.readInt()];//?
        for (int i = 0; i < data.length; i++) {
            data[i] = input.readFloat();
        }
    }

    @Override
    public int bufferLen() {
        return 4 + data.length * 4;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        output.writeFloat(data.length);
        for (int i = 0; i < data.length; i++) {
            output.writeFloat(data[i]);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        data = new float[input.readInt()];
        for (int i = 0; i < data.length; i++) {
            data[i] = input.readFloat();
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
