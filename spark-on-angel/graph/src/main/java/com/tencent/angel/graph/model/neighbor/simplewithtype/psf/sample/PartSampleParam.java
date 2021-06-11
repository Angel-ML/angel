package com.tencent.angel.graph.model.neighbor.simplewithtype.psf.sample;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

/**
 * General partition get parameter
 */
public class PartSampleParam extends GeneralPartGetParam {

  protected int sampleType;

  public PartSampleParam(int matrixId, PartitionKey partKey, KeyPart indicesPart, int sampleType) {
    super(matrixId, partKey, indicesPart);
    this.sampleType = sampleType;
  }

  public PartSampleParam() {
    this(-1, null, null, -1);
  }

  public int getSampleType() {
    return sampleType;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, sampleType);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    sampleType = ByteBufSerdeUtils.deserializeInt(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH;
  }

}
