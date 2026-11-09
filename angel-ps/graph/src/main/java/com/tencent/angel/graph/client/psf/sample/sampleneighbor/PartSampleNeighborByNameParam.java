package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborByNameParam extends PartSampleNeighborParam {

  private String name;

  public PartSampleNeighborByNameParam(int matrixId, PartitionKey part, KeyPart indicesPart,
      int count, String name, SampleMethod method, NeighborStorageType storageType) {
    super(matrixId, part, indicesPart, count, method, storageType);
    this.name = name;
  }

  public PartSampleNeighborByNameParam() {
    this(-1, null, null, -1, "", SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeBytes(buf, name.getBytes());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    name = new String(ByteBufSerdeUtils.deserializeBytes(buf));
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedBytesLen(name.getBytes());
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }
}