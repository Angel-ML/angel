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

    private float[] embeddings;
    private float[] slotsValues;
    private int numSlots;

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
        float[] cloneEmbeddings = new float[embeddings.length];
        System.arraycopy(embeddings, 0, cloneEmbeddings, 0, embeddings.length);
        float[] cloneSlotsValues = new float[slotsValues.length];
        System.arraycopy(slotsValues, 0, cloneSlotsValues, 0, slotsValues.length);

        return new UniversalEmbeddingNode(cloneEmbeddings, cloneSlotsValues, numSlots);
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
