package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartUniversalEmbeddingUpdateParam extends GeneralPartUpdateParam {

    private float[] floats;

    private int[] ints;

    public PartUniversalEmbeddingUpdateParam(int matrixId, PartitionKey partKey, KeyValuePart
            keyValuePart, float[] floats, int[] ints) {
        super(matrixId, partKey, keyValuePart);
        this.floats = floats;
        this.ints = ints;
    }

    public PartUniversalEmbeddingUpdateParam() {
        this(-1, null, null, null, null);
    }

    public float[] getFloats() {
        return floats;
    }

    public int[] getInts() {
        return ints;
    }

    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        ByteBufSerdeUtils.serializeFloats(buf, floats);
        ByteBufSerdeUtils.serializeInts(buf, ints);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        floats = ByteBufSerdeUtils.deserializeFloats(buf);
        ints = ByteBufSerdeUtils.deserializeInts(buf);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.serializedFloatsLen(floats)
                + ByteBufSerdeUtils.serializedIntsLen(ints);
    }
}
