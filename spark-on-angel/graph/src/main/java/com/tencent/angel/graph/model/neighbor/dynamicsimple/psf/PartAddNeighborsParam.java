package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

public class PartAddNeighborsParam extends PartitionUpdateParam {
    private KeyValuePart keyValuePart;
    private boolean isAttempt;

    public PartAddNeighborsParam(int matrixId, PartitionKey partKey, KeyValuePart keyValuePart, boolean isAttempt) {
        super(matrixId, partKey);
        this.keyValuePart = keyValuePart;
        this.isAttempt = isAttempt;
    }

    public PartAddNeighborsParam() {
        this(-1, null, null, false);
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        ByteBufSerdeUtils.serializeKeyValuePart(buf, keyValuePart);
        ByteBufSerdeUtils.serializeBoolean(buf, isAttempt);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        keyValuePart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
        isAttempt = ByteBufSerdeUtils.deserializeBoolean(buf);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.serializedKeyValuePartLen(keyValuePart)
                + ByteBufSerdeUtils.serializedBooleanLen(isAttempt);
    }

    public KeyValuePart getKeyValuePart() {
        return keyValuePart;
    }

    public boolean getIsAttempt() { return isAttempt; }

    public void setKeyValuePart(KeyValuePart keyValuePart) {
        this.keyValuePart = keyValuePart;
    }

    public void setIsAttempt(boolean isAttempt) { this.isAttempt = isAttempt; }
}
