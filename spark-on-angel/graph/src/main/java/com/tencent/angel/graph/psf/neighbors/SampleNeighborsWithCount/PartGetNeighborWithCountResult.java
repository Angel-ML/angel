package com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNeighborWithCountResult extends PartitionGetResult {


  private long[] nodeIds;
  private long[][] neighbors;

  public PartGetNeighborWithCountResult(long[] nodeIds, long[][] neighbors) {

    this.nodeIds = nodeIds;
    this.neighbors = neighbors;
  }

  public PartGetNeighborWithCountResult() {

  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
    ByteBufSerdeUtils.serialize2DLongs(output, neighbors);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
    neighbors = ByteBufSerdeUtils.deserialize2DLongs(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(nodeIds) + ByteBufSerdeUtils
        .serialized2DLongsLen(neighbors);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public long[][] getData() {
    return neighbors;
  }

  public void setData(long[][] data) {
    this.neighbors = data;
  }
}
