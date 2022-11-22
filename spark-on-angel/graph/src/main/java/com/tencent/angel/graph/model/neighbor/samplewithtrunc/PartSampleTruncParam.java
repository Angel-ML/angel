package com.tencent.angel.graph.model.neighbor.samplewithtrunc;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartSampleTruncParam extends GeneralPartGetParam {

    protected boolean isTrunc;
    protected  int truncLen;
    private boolean dynamicInitNeighbor;

    public PartSampleTruncParam(int matrixId, PartitionKey partKey, KeyPart indicesPart, boolean isTrunc, int truncLen) {
        super(matrixId, partKey, indicesPart);
        this.isTrunc = isTrunc;
        this.truncLen = truncLen;
        this.dynamicInitNeighbor = false;
    }

    public PartSampleTruncParam(int matrixId, PartitionKey partKey, KeyPart indicesPart, boolean isTrunc, int truncLen, boolean dynamicInitNeighbor) {
        super(matrixId, partKey, indicesPart);
        this.isTrunc = isTrunc;
        this.truncLen = truncLen;
        this.dynamicInitNeighbor = dynamicInitNeighbor;
    }

    public PartSampleTruncParam() {this(-1, null, null, false, -1, false);}

    public boolean getIsTrunc() {return isTrunc;}

    public int getTruncLen() {return truncLen;}

    public boolean getDynamicInitNeighbor() {return dynamicInitNeighbor;}

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        ByteBufSerdeUtils.serializeBoolean(buf, isTrunc);
        ByteBufSerdeUtils.serializeInt(buf, truncLen);
        ByteBufSerdeUtils.serializeBoolean(buf, dynamicInitNeighbor);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        isTrunc = ByteBufSerdeUtils.deserializeBoolean(buf);
        truncLen = ByteBufSerdeUtils.deserializeInt(buf);
        dynamicInitNeighbor = ByteBufSerdeUtils.deserializeBoolean(buf);;
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.BOOLEN_LENGTH * 2 + ByteBufSerdeUtils.INT_LENGTH;
    }
}