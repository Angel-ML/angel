package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborWithTypeParam extends PartSampleNeighborParam {

  /**
   * Sample type
   */
  private SampleType sampleType;

  public PartSampleNeighborWithTypeParam(int matrixId, PartitionKey part, KeyPart indicesPart,
      int count, SampleType sampleType, SampleMethod method, NeighborStorageType storageType) {
    super(matrixId, part, indicesPart, count, method, storageType);
    this.sampleType = sampleType;
  }

  public PartSampleNeighborWithTypeParam() {
    this(-1, null, null, -1, SampleType.SIMPLE, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeInt(buf, sampleType.getIndex());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    sampleType = SampleType.valueOf(ByteBufSerdeUtils.deserializeInt(buf));
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH;
  }

  public SampleType getSampleType() {
    return sampleType;
  }

  public void setSampleType(SampleType sampleType) {
    this.sampleType = sampleType;
  }
}
