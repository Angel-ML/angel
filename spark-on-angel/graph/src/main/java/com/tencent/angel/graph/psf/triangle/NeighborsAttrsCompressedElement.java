package com.tencent.angel.graph.psf.triangle;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class NeighborsAttrsCompressedElement implements IElement {
    private static final Logger LOG = LoggerFactory.getLogger(NeighborsAttrsCompressedElement.class);

    private byte[] neighborIdsCompressed;
    private byte[] attrs;

    public NeighborsAttrsCompressedElement() {
        this(null, null);
    }

    public NeighborsAttrsCompressedElement(byte[] neighborIdsCompressed, byte[] attrs) {
        this.neighborIdsCompressed = neighborIdsCompressed;
        this.attrs = attrs;
    }

    public long[] getNeighborIds() {
        if (getNumNodes() == 0) return null;

        long[] result = null;
        try {
            result = Snappy.uncompressLongArray(neighborIdsCompressed);
        } catch (IOException e) {
            LOG.error("uncompress neighbor IDs error! neighborLen: {}, attrLen: {}", neighborIdsCompressed.length, attrs.length);
        }
        return result;
    }

    public int getNeighborIdsCompressedSize() {
        return neighborIdsCompressed == null ? 0 : neighborIdsCompressed.length;
    }

    public byte[] getAttrs() {
        return attrs;
    }

    public int getNumNodes() {
        return attrs.length;
    }

    @Override
    public Object deepClone() {
        int len = neighborIdsCompressed.length;
        byte[] nodeIdsCompressed = new byte[len];
        byte[] newAttrs = new byte[len];
        System.arraycopy(neighborIdsCompressed, 0, nodeIdsCompressed, 0, len);
        System.arraycopy(attrs, 0, newAttrs, 0, len);
        return new NeighborsAttrsCompressedElement(nodeIdsCompressed, newAttrs);
    }

    @Override
    public void serialize(ByteBuf output) {
        int neighborLen = neighborIdsCompressed.length;
        int attrLen = attrs.length;
        output.writeInt(neighborLen);
        output.writeInt(attrLen);

        for (int i = 0; i < neighborLen; i++) {
            output.writeByte(neighborIdsCompressed[i]);
        }
        for (int i = 0; i < attrLen; i++) {
            output.writeByte(attrs[i]);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        int neighborLen = input.readInt();
        int attrLen = input.readInt();
        neighborIdsCompressed = new byte[neighborLen];
        attrs = new byte[attrLen];

        for (int i = 0; i < neighborLen; i++) {
            neighborIdsCompressed[i] = input.readByte();
        }
        for (int i = 0; i < attrLen; i++) {
            attrs[i] = input.readByte();
        }
    }

    @Override
    public int bufferLen() {
        return Integer.BYTES * 2 + neighborIdsCompressed.length * Byte.BYTES + attrs.length * Byte.BYTES;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        int neighborLen = neighborIdsCompressed.length;
        int attrLen = attrs.length;
        output.writeInt(neighborLen);
        output.writeInt(attrLen);

        for (int i = 0; i < neighborLen; i++) {
            output.writeByte(neighborIdsCompressed[i]);
        }
        for (int i = 0; i < attrLen; i++) {
            output.writeByte(attrs[i]);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        int neighborLen = input.readInt();
        int attrLen = input.readInt();
        neighborIdsCompressed = new byte[neighborLen];
        attrs = new byte[attrLen];

        for (int i = 0; i < neighborLen; i++) {
            neighborIdsCompressed[i] = input.readByte();
        }
        for (int i = 0; i < attrLen; i++) {
            attrs[i] = input.readByte();
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
