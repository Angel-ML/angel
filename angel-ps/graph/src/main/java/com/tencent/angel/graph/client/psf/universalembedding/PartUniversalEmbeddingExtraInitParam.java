package com.tencent.angel.graph.client.psf.universalembedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartUniversalEmbeddingExtraInitParam extends GeneralPartUpdateParam {

    private int dim;

    private int numSlots;

    public PartUniversalEmbeddingExtraInitParam(int matrixId, PartitionKey partKey,
                                                KeyValuePart keyValuePart, int dim, int numSlots) {
        super(matrixId, partKey, keyValuePart);
        this.dim = dim;
        this.numSlots = numSlots;
    }

    public PartUniversalEmbeddingExtraInitParam() {
        this(-1, null, null, -1, -1);
    }

    public int getDim() {
        return dim;
    }

    public int getNumSlots() {
        return numSlots;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        ByteBufSerdeUtils.serializeInt(buf, dim);
        ByteBufSerdeUtils.serializeInt(buf, numSlots);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        dim = ByteBufSerdeUtils.deserializeInt(buf);
        numSlots = ByteBufSerdeUtils.deserializeInt(buf);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH * 2;
    }
}
