package com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import io.netty.buffer.ByteBuf;

/**
 * General partition get parameter
 */
public class PartGetNeighborWithCountParam extends PartitionGetParam {

  /**
   * Indices partition
   */
  protected KeyValuePart indicesPart;

  public PartGetNeighborWithCountParam(int matrixId, PartitionKey partKey,
      KeyValuePart indicesPart) {
    super(matrixId, partKey);
    this.indicesPart = indicesPart;
  }

  public PartGetNeighborWithCountParam() {
    this(-1, null, null);
  }

  public KeyValuePart getIndicesPart() {
    return indicesPart;
  }

  public void setIndicesPart(KeyValuePart indicesPart) {
    this.indicesPart = indicesPart;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    ByteBufSerdeUtils.serializeKeyValuePart(buf, indicesPart);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    indicesPart = ByteBufSerdeUtils.deserializeKeyValuePart(buf);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedKeyValuePartLen(indicesPart);
  }

}
