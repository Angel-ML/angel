package com.tencent.angel.ps.storage.vector.element;

import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LongArrayIntElement implements IElement {

    private long[][] data;
    private int partNum;

    public LongArrayIntElement(long[][] data, int partNum) {
        this.data = data;
        this.partNum = partNum;
    }

    public LongArrayIntElement() {
        this(null, -1);
    }

    public long[][] getData() {
        return data; 
    }

    public int getPartNum() {
        return partNum;
    }

    public void setDataPart(long[][] data, int partNum) {
        this.data = data;
        this.partNum = partNum;
    }

    @Override
    public LongArrayIntElement deepClone() {
        long[][] newData = new long[data.length][];
        int newPartNum = partNum;
        System.arraycopy(data, 0, newData, 0, data.length);
        return new LongArrayIntElement(newData, newPartNum);
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(data.length);
        for (int i = 0; i < data.length; i++) {
            output.writeInt(data[i].length);
            for (int j = 0; j < data[i].length; j++) {
                output.writeLong(data[i][j]);
            }
        }
        output.writeInt(partNum);
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = new long[input.readInt()][];
        for (int i = 0; i < data.length; i++) {
            int len = input.readInt();
            for (int j = 0; j < len; j++) {
                data[i][j] = input.readLong();
            }
        }
        partNum = input.readInt();
    }

    @Override
    public int bufferLen() {
        int len = 4 + 4;
        for (int i = 0; i < data.length; i++) {
            len += (8 * data[i].length + 4);
        }
        return len;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        output.writeInt(data.length);
        for (int i = 0; i < data.length; i++) {
            output.writeInt(data[i].length);
            for (int j = 0; j < data[i].length; j++) {
                output.writeLong(data[i][j]);
            }
        }
        output.writeInt(partNum);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        for (int i = 0; i < data.length; i++) {
            int len = input.readInt();
            for (int j = 0; j < len; j++) {
                data[i][j] = input.readLong();
            }
        }
        partNum = input.readInt();
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
