package com.tencent.angel.graph.client.getnodefeats2;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNodeFeatsParam extends PartitionGetParam {

  /**
   * Node ids
   */
  private long[] nodeIds;

  private int startIndex;
  private int endIndex;


  public PartGetNodeFeatsParam(int matrixId, PartitionKey part, long[] nodeIds
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartGetNodeFeatsParam() {
    this(-1, null, null, -1, -1);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
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
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    nodeIds = new long[buf.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 8 * nodeIds.length;
  }
}
