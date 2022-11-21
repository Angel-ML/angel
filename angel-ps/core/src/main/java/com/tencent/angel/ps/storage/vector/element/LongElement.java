package com.tencent.angel.ps.storage.vector.element;

import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * The complex object that contains long ele, it can be stored in PS
 */
public class LongElement implements IElement {

    private long data;

    public LongElement(long data) {
        this.data = data;
    }

    public LongElement() {
        this(-1);
    }

    public long getData() {
        return data;
    }

    public void setData(long data) {
        this.data = data;
    }

    @Override
    public LongElement deepClone() {
        return new LongElement(data);
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeLong(data);
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = input.readLong();
    }

    @Override
    public int bufferLen() {
        return 8;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        output.writeLong(data);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        data = input.readLong();
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
