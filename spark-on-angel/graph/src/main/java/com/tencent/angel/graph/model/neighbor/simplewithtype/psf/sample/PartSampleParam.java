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
  protected boolean sampleByType;

  public PartSampleParam(int matrixId, PartitionKey partKey, KeyPart indicesPart, int sampleType, boolean sampleByType) {
    super(matrixId, partKey, indicesPart);
    this.sampleType = sampleType;
    this.sampleByType = sampleByType;
  }

  public PartSampleParam() {
    this(-1, null, null, -1, true);
  }

  public int getSampleType() {
    return sampleType;
  }

  public boolean getSampleByType() { return sampleByType; }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, sampleType);
    ByteBufSerdeUtils.serializeBoolean(buf, sampleByType);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    sampleType = ByteBufSerdeUtils.deserializeInt(buf);
    sampleByType = ByteBufSerdeUtils.deserializeBoolean(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.BOOLEN_LENGTH;
  }

}
