package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleWithTagByNodeIdResult extends PartitionGetResult {
  private int partId;
  private long[] nodeIds;
  private long[] neighbors;

  public PartSampleWithTagByNodeIdResult(int partitionId, long[] nodeIds, long[] neighbors) {
    this.partId = partitionId;
    this.nodeIds = nodeIds;
    this.neighbors = neighbors;
  }

  public PartSampleWithTagByNodeIdResult() {}

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
    ByteBufSerdeUtils.serializeLongs(output, neighbors);
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
    neighbors = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedIntLen(partId) + ByteBufSerdeUtils.serializedLongsLen(nodeIds) + ByteBufSerdeUtils.serializedLongsLen(neighbors);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public long[] getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(long[] neighbors) {
    this.neighbors = neighbors;
  }

  public int getPartId() { return partId; }
}
