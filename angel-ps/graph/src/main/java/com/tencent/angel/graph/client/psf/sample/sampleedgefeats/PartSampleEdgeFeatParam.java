package com.tencent.angel.graph.client.psf.sample.sampleedgefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.PartSampleNeighborParam;
import com.tencent.angel.graph.client.psf.sample.sampleneighbor.SampleMethod;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import io.netty.buffer.ByteBuf;

public class PartSampleEdgeFeatParam extends PartSampleNeighborParam {

  public PartSampleEdgeFeatParam(int matrixId, PartitionKey part, KeyPart indicesPart, int count,
      SampleMethod method, NeighborStorageType storageType) {
    super(matrixId, part, indicesPart, count, method, storageType);
  }

  public PartSampleEdgeFeatParam() {
    this(0, null, null, 0, SampleMethod.RANDOM, NeighborStorageType.LONGARRAY);
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen();
  }
}
