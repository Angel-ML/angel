package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A user-define data type that store embedding lookup matrix on PS
 */
public class UniversalEmbeddingNode implements IElement {

    public float[] embeddings;
    public float[] slotsValues;
    public int numSlots;

    public UniversalEmbeddingNode(float[] embeddings, float[] slotsValues, int numSlots) {
        this.embeddings = embeddings;
        this.slotsValues = slotsValues;
        this.numSlots = numSlots;
    }

    public UniversalEmbeddingNode(float[] embeddings) {
        this(embeddings, null, 1);
    }

    public UniversalEmbeddingNode() {
        this(null, null, -1);
    }

    public float[] getEmbeddings() {
        return embeddings;
    }

    public void setEmbeddings(float[] embeddings) {
        this.embeddings = embeddings;
    }

    public float[] getSlotsValues() {
        return slotsValues;
    }

    public void setSlotsValues(float[] slotsValues) {
        this.slotsValues = slotsValues;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public void setNumSlots(int numSlots) {
        this.numSlots = numSlots;
    }

    @Override
    public Object deepClone() {
        // Both arrays are optional everywhere else in this class: the no-arg and
        // the single-arg constructors leave slotsValues (and possibly embeddings)
        // null, serialize/deserialize/bufferLen all guard them with a presence
        // flag, and the standard init path stores a slot-less node whenever
        // numSlots <= 1 (see UniversalEmbeddingWithFieldInitAsNodes). Cloning
        // must therefore tolerate null instead of dereferencing blindly.
        return new UniversalEmbeddingNode(copyFloats(embeddings), copyFloats(slotsValues), numSlots);
    }

    /**
     * Null-preserving array copy.
     *
     * A null array must stay null in the clone: turning it into an empty array
     * would make the clone serialize differently from the original (presence
     * flag 1 + length 0 instead of flag 0) and change bufferLen accordingly.
     */
    private static float[] copyFloats(float[] src) {
        if (src == null) {
            return null;
        }
        float[] dst = new float[src.length];
        System.arraycopy(src, 0, dst, 0, src.length);
        return dst;
    }

    @Override
    public void serialize(ByteBuf output) {
        if (embeddings != null) {
            ByteBufSerdeUtils.serializeInt(output, 1);
            ByteBufSerdeUtils.serializeFloats(output, embeddings);
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }

        if (slotsValues != null) {
            ByteBufSerdeUtils.serializeInt(output, 1);
            ByteBufSerdeUtils.serializeFloats(output, slotsValues);
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }

        ByteBufSerdeUtils.serializeInt(output, numSlots);
    }

    @Override
    public void deserialize(ByteBuf input) {
        int embeddingsFlag = ByteBufSerdeUtils.deserializeInt(input);
        if (embeddingsFlag > 0) {
            embeddings = ByteBufSerdeUtils.deserializeFloats(input);
        }

        int slotsValuesFlag = ByteBufSerdeUtils.deserializeInt(input);
        if (slotsValuesFlag > 0) {
            slotsValues = ByteBufSerdeUtils.deserializeFloats(input);
        }

        numSlots = ByteBufSerdeUtils.deserializeInt(input);
    }

    @Override
    public int bufferLen() {
        // add flag len
        int len = ByteBufSerdeUtils.INT_LENGTH * 2;

        if (embeddings != null) {
            len += ByteBufSerdeUtils.serializedFloatsLen(embeddings);
        }

        if (slotsValues != null) {
            len += ByteBufSerdeUtils.serializedFloatsLen(slotsValues);
        }

        len += ByteBufSerdeUtils.INT_LENGTH;

        return len;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        if (embeddings != null) {
            StreamSerdeUtils.serializeInt(output, 1);
            StreamSerdeUtils.serializeFloats(output, embeddings);
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }

        if (slotsValues != null) {
            StreamSerdeUtils.serializeInt(output, 1);
            StreamSerdeUtils.serializeFloats(output, slotsValues);
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }

        StreamSerdeUtils.serializeInt(output, numSlots);

    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        int embeddingsFlag = StreamSerdeUtils.deserializeInt(input);
        if (embeddingsFlag > 0) {
            embeddings = StreamSerdeUtils.deserializeFloats(input);
        }

        int slotsValuesFlag = StreamSerdeUtils.deserializeInt(input);
        if (slotsValuesFlag > 0) {
            slotsValues = StreamSerdeUtils.deserializeFloats(input);
        }

        numSlots = StreamSerdeUtils.deserializeInt(input);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
