package com.tencent.angel.spark.ml.tree.util;

public class BufferedBitSetWriter {
    //private final SerializableBuffer bytes;
    //private final ByteBuffer bytes;
    private final byte[] bytes;
    private int writeIndex;
    private int writeOffset;
    private int writeBuffer;

    public BufferedBitSetWriter(int capacity) {
        int numBytes = (int) Math.ceil(1.0 * capacity / 8);
        //this.bytes = ByteBuffer.allocate(numBytes);
        this.bytes = new byte[numBytes];
    }

    //public BufferedBitSetWriter(ByteBuffer bytes) {
    //    this.bytes = bytes;
    //}

    public BufferedBitSetWriter(byte[] bytes) {
        this.bytes = bytes;
    }

    public void write(boolean bit) {
        writeBuffer = writeBuffer << 1;
        if (bit)
            writeBuffer |= 1;
        if (++writeOffset == 8) {
            writeToBuffer();
        }
    }

    private void writeToBuffer() {
        //bytes.put(writeIndex++, (byte) writeBuffer);
        bytes[writeIndex++] = (byte) writeBuffer;
        writeOffset = 0;
        writeBuffer = 0;
    }

    public void complete() {
        if (writeOffset != 0) {
            writeBuffer <<= 8 - writeOffset;
            writeToBuffer();
        }
    }

    //public ByteBuffer getBytes() {
    //    return bytes;
    //}


    public byte[] getBytes() {
        return bytes;
    }
}
