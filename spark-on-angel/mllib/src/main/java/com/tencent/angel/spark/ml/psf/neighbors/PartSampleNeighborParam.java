package com.tencent.angel.spark.ml.psf.neighbors;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartSampleNeighborParam extends PartitionGetParam {

  private long[] nodeIds;

  private int count;

  private int startIndex;

  private int endIndex;

  public PartSampleNeighborParam(int matrixId, PartitionKey part, int count, long[] nodeIds
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.count = count;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartSampleNeighborParam() {
    this(0, null, 0, null, 0, 0);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public int getCount() {
    return count;
  }

  public void setCount(int count) {
    this.count = count;
  }

  public int getStartIndex() {
    return startIndex;
  }

  public int getEndIndex() {
    return endIndex;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(count);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = buf.readInt();
    nodeIds = new long[buf.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 * 2 + 8 * (endIndex - startIndex);
  }

}
