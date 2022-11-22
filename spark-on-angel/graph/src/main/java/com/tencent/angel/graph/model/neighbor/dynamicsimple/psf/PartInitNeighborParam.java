package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartInitNeighborParam extends PartitionUpdateParam {
    protected KeyValuePart indicesPart;
    private boolean isWeighted;

    public PartInitNeighborParam(int matrixId, PartitionKey partKey,
                                 KeyValuePart indicesPart, boolean isWeighted) {
        super(matrixId, partKey);
        this.indicesPart = indicesPart;
        this.isWeighted = isWeighted;
    }

    public PartInitNeighborParam() {
        this(-1, null, null, false);
    }

    public KeyValuePart getIndicesPart() {
        return indicesPart;
    }

    public boolean getIsWeighted() { return isWeighted; }

    public void setIndicesPart(KeyValuePart indicesPart) {
        this.indicesPart = indicesPart;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        ByteBufSerdeUtils.serializeKeyValuePart(buf, indicesPart);
        ByteBufSerdeUtils.serializeBoolean(buf, isWeighted);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        indicesPart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
        isWeighted = ByteBufSerdeUtils.deserializeBoolean(buf);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.serializedKeyValuePartLen(indicesPart)
                + ByteBufSerdeUtils.serializedBooleanLen(isWeighted);
    }

}
