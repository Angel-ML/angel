package com.tencent.angel.spark.ml.ann_core;

import com.tencent.angel.common.collections.DynamicObjectArray;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NearestNeighborsIElement implements IElement {

    private NeighborDistance[] foundKeysDis;

    public NearestNeighborsIElement(NeighborDistance[] foundKeysDis) {
        this.foundKeysDis = foundKeysDis;
    }

    public NearestNeighborsIElement() {
        this(null);
    }

    public NeighborDistance[] getFoundKeysDis() {
        return this.foundKeysDis;
    }

    public void setFoundKeysDis(NeighborDistance[] foundKeysDis) {
        this.foundKeysDis = foundKeysDis;
    }

    public NearestNeighborsIElement merge(NearestNeighborsIElement other, int topK) {
        Arrays.sort(foundKeysDis, new java.util.Comparator<NeighborDistance>() {
            @Override
            public int compare(NeighborDistance a, NeighborDistance b) {
                return Float.compare(a.getDistance(), b.getDistance());
            }
        });
        Arrays.sort(other.getFoundKeysDis(), new java.util.Comparator<NeighborDistance>() {
            @Override
            public int compare(NeighborDistance a, NeighborDistance b) {
                return Float.compare(a.getDistance(), b.getDistance());
            }
        });

        List<NeighborDistance> merged = new ArrayList<>();

        int pthis = 0;
        int pthat = 0;
        int len_this = foundKeysDis.length;
        int len_that = other.getFoundKeysDis().length;
        for (int i = 0; i < topK && pthat < len_that && pthis < len_this; i += 1) {
            if (foundKeysDis[pthis].getDistance() <= other.getFoundKeysDis()[pthat].getDistance()) {
                merged.add(foundKeysDis[pthis]);
                pthis += 1;
            }
            else {
                merged.add(other.getFoundKeysDis()[pthat]);
                pthat += 1;
            }
        }

        while (merged.size() < topK) {
            if (pthis < len_this) {
                merged.add(foundKeysDis[pthis]);
                pthis += 1;
            }
            else if (pthat < len_that) {
                merged.add(other.getFoundKeysDis()[pthat]);
                pthat += 1;
            }
            else {
                break;
            }
        }

        return new NearestNeighborsIElement(merged.toArray(new NeighborDistance[0]));
    }

    @Override
    public Object deepClone() {
        NeighborDistance[] cloneFoundKeysDis = new NeighborDistance[foundKeysDis.length];
        System.arraycopy(foundKeysDis, 0, cloneFoundKeysDis, 0, foundKeysDis.length);

        return new NearestNeighborsIElement(cloneFoundKeysDis);
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(foundKeysDis.length);
        for (int i = 0; i < foundKeysDis.length; i += 1) {
            output.writeLong(foundKeysDis[i].getNeighbor());
            output.writeFloat(foundKeysDis[i].getDistance());
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        foundKeysDis = new NeighborDistance[input.readInt()];
        for (int i = 0; i < foundKeysDis.length; i += 1) {
            long key = input.readLong();
            float dis = input.readFloat();
            foundKeysDis[i] = new NeighborDistance(key, dis);
        }
    }

    @Override
    public int bufferLen() {
        return 4 + foundKeysDis.length * 12;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        byte[] data = new byte[bufferLen()];
        int index = 0;
        output.writeInt(data.length);
        index = writeInt(data, foundKeysDis.length, index);
        for (int i = 0; i < foundKeysDis.length; i += 1) {
            index = writeLong(data, foundKeysDis[i].getNeighbor(), index);
            index = writeFloat(data, foundKeysDis[i].getDistance(), index);
        }

        output.write(data);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        byte[] data = new byte[input.readInt()];
        input.readFully(data);
        int index = 0;

        int foundKeysDisLen = readInt(data, index);
        foundKeysDis = new NeighborDistance[foundKeysDisLen];
        index += 4;

        for (int i = 0; i < foundKeysDisLen; i += 1) {
            long key = readLong(data, index);
            index += 8;
            float dis = readFloat(data, index);
            index += 4;
            foundKeysDis[i] = new NeighborDistance(key, dis);
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

    private int writeInt(byte[] data, int v, int index) {
        data[index] = (byte)((v >>> 24) & 0xFF);
        data[index + 1] = (byte)((v >>> 16) & 0xFF);
        data[index + 2] = (byte)((v >>> 8) & 0xFF);
        data[index + 3] = (byte)((v >>> 0) & 0xFF);
        return index + 4;
    }

    private int writeLong(byte[] data, long v, int index) {
        data[index] = (byte)((v >>> 56) & 0xFF);
        data[index + 1] = (byte)((v >>> 48) & 0xFF);
        data[index + 2] = (byte)((v >>> 40) & 0xFF);
        data[index + 3] = (byte)((v >>> 32) & 0xFF);
        data[index + 4] = (byte)((v >>> 24) & 0xFF);
        data[index + 5] = (byte)((v >>> 16) & 0xFF);
        data[index + 6] = (byte)((v >>> 8) & 0xFF);
        data[index + 7] = (byte)((v >>> 0) & 0xFF);

        return index + 8;
    }

    private int writeFloat(byte[] data, float v, int index) {
        int iv = Float.floatToIntBits(v);
        return writeInt(data, iv, index);
    }

    private int readInt(byte[] data, int index) {
        int ch1 = data[index] & 255;
        int ch2 = data[index + 1] & 255;
        int ch3 = data[index + 2] & 255;
        int ch4 = data[index + 3] & 255;
        int c =  (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4);
        return c;
    }

    private long readLong(byte[] data, int index) {
        long ch1 = data[index] & 255;
        long ch2 = data[index + 1] & 255;
        long ch3 = data[index + 2] & 255;
        long ch4 = data[index + 3] & 255;
        long ch5 = data[index + 4] & 255;
        long ch6 = data[index + 5] & 255;
        long ch7 = data[index + 6] & 255;
        long ch8 = data[index + 7] & 255;
        long c = (ch1 << 56) + (ch2 << 48) + (ch3 << 40) + (ch4 << 32) +
                (ch5 << 24) + (ch6 << 16) + (ch7 << 8) + ch8;
        return c;
    }

    private float readFloat(byte[] data, int index) {
        return Float.intBitsToFloat(readInt(data, index));
    }

}
