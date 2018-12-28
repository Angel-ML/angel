package com.tencent.angel.spark.ml.tree.util;

import java.nio.ByteBuffer;
import java.util.Random;

public class BufferedBitSet {
    private final ByteBuffer bytes;
    private int writeIndex;
    private int writeOffset;
    private int writeBuffer;
    private int readIndex;
    private int readMask;
    private int readBuffer;
    private int readIndexT;
    private int readMaskT;
    private int readBufferT;

    public BufferedBitSet(ByteBuffer bytes, int numBits) {
        this.bytes = bytes;
        int capacity = bytes.capacity() * 8;
        readIndexT = bytes.capacity() - 1;
        for (int i = numBits; i < capacity; i++)
            readTail();
        //System.out.println("----------");
    }

    public BufferedBitSet(int capacity) {
        int numBytes = (int) Math.ceil(1.0 * capacity / 8);
        this.bytes = ByteBuffer.allocate(numBytes);
    }

    public void write(boolean bit) {
        writeBuffer = writeBuffer << 1;
        if (bit)
            writeBuffer |= 1;
        if (++writeOffset == 8) {
            writeToBuffer();
        }
    }

    public boolean read() {
        if (readMask == 0) {
            readBuffer = bytes.get(readIndex++);
            readMask = 0b10000000;
        }
        boolean bit = (readBuffer & readMask) > 0;
        readMask >>= 1;
        return bit;
    }

    public boolean readTail() {
        if (readMaskT == 0) {
            readBufferT = bytes.get(readIndexT--);
            readMaskT = 0b00000001;
        }
        //System.out.println("read buffer = " + Integer.toBinaryString(readBufferT & 0b11111111)
        //        + ", mask = " + Integer.toBinaryString(readMaskT));
        boolean bit = (readBufferT & readMaskT) > 0;
        readMaskT = (readMaskT << 1) & 0b11111111;
        //readMaskT <<= 1;
        return bit;
    }

    private void writeToBuffer() {
        bytes.put(writeIndex++, (byte) writeBuffer);
        writeOffset = 0;
        writeBuffer = 0;
    }

    public boolean get(int index) {
        int x = index >> 3;
        int y = index & 0b111;
        return ((bytes.get(x) >> (7 - y)) & 0x1) == 1;
    }

    public void complete() {
        if (writeOffset != 0) {
            writeBuffer <<= 8 - writeOffset;
            writeToBuffer();
        }
    }

    public ByteBuffer getBytes() {
        return bytes;
    }

    public int getCapacity() {
        return bytes.capacity() * 8;
    }

    public static void main(String[] args) {
        Random random = new Random();

        int n = 100000003;
        boolean[] bits = new boolean[n];
        for (int i = 0; i < n; i++)
            bits[i] = random.nextBoolean();

        //boolean[] bits = new boolean[]{true, true, false, false, true, false, false, false, true, true, false, true};
        //int n = bits.length;

        //BufferedBitSet writeBitSet = new BufferedBitSet(n);
        BufferedBitSetWriter writeBitSet = new BufferedBitSetWriter(n);
        for (int i = 0; i < n; i++)
            writeBitSet.write(bits[i]);
        writeBitSet.complete();

        //BufferedBitSet readBitSet = new BufferedBitSet(writeBitSet.getBytes(), n);
        BufferedBitSetReader readBitSet = new BufferedBitSetReader(writeBitSet.getBytes(), n);
        int h = 0, t = n - 1;
        for (int i = 0; i < n; i++) {
            if (random.nextBoolean()) {
                boolean bit = readBitSet.readHead();
                if (bit != bits[h])
                    throw new RuntimeException("head " + h);
                h++;
            } else {
                boolean bit = readBitSet.readTail();
                if (bit != bits[t])
                    throw new RuntimeException("tail " + t);
                t--;
            }
            //if (bitSet.get(i) != bits[i]) {
            //    throw new RuntimeException("" + i);
            //}
        }

        for (int i = 0; i < n; i++) {
            if (readBitSet.read(i) != bits[i])
                throw new RuntimeException("" + i);
        }
    }

}
