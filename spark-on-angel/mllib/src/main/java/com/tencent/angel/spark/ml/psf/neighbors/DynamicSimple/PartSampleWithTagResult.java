package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleWithTagResult extends PartitionGetResult {
  private int partId;
  private long[] nodeIds;

  public PartSampleWithTagResult(int partitionId, long[] nodeIds) {
    this.partId = partitionId;
    this.nodeIds = nodeIds;
  }

  public PartSampleWithTagResult() {}

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedIntLen(partId) + ByteBufSerdeUtils.serializedLongsLen(nodeIds);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public int getPartId() { return partId; }
}
