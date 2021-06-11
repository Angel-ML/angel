package com.tencent.angel.graph.common.psf.result;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetLongsResult extends PartitionGetResult {

  private long[] nodeIds;
  private long[][] data;

  public PartGetLongsResult(long[] nodeIds, long[][] data) {
    this.nodeIds = nodeIds;
    this.data = data;
  }

  public PartGetLongsResult() {
    this(null, null);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
    ByteBufSerdeUtils.serialize2DLongs(output, data);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
    data = ByteBufSerdeUtils.deserialize2DLongs(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(nodeIds) + ByteBufSerdeUtils
        .serialized2DLongsLen(data);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public long[][] getData() {
    return data;
  }

  public void setData(long[][] data) {
    this.data = data;
  }
}
