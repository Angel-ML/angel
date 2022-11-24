package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartInitParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartUniversalEmbeddingInitParam extends GeneralPartInitParam {

    private int seed;

    private int dim;

    private int numSlots;

    private float[] floats;

    private int[] ints;

    public PartUniversalEmbeddingInitParam(int matrixId, PartitionKey partKey, KeyPart indicesPart,
                                           int seed, int dim, int numSlots, float[] floats, int[] ints) {
        super(matrixId, partKey, indicesPart);
        this.seed = seed;
        this.dim = dim;
        this.numSlots = numSlots;
        this.floats = floats;
        this.ints = ints;
    }

    public PartUniversalEmbeddingInitParam() {
        this(-1, null, null, -1, -1, -1, null, null);
    }

    public int getSeed() {
        return seed;
    }

    public int getDim() {
        return dim;
    }

    public int getNumSlots() {
        return numSlots;
    }

    public float[] getFloats() {
        return floats;
    }

    public int[] getInts() {
        return ints;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        ByteBufSerdeUtils.serializeInt(buf, seed);
        ByteBufSerdeUtils.serializeInt(buf, dim);
        ByteBufSerdeUtils.serializeInt(buf, numSlots);
        ByteBufSerdeUtils.serializeFloats(buf, floats);
        ByteBufSerdeUtils.serializeInts(buf, ints);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        seed = ByteBufSerdeUtils.deserializeInt(buf);
        dim = ByteBufSerdeUtils.deserializeInt(buf);
        numSlots = ByteBufSerdeUtils.deserializeInt(buf);
        floats = ByteBufSerdeUtils.deserializeFloats(buf);
        ints = ByteBufSerdeUtils.deserializeInts(buf);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH * 3
                + ByteBufSerdeUtils.serializedFloatsLen(floats)
                + ByteBufSerdeUtils.serializedIntsLen(ints);
    }
}
