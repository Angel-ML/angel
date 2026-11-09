package com.tencent.angel.spark.ml.psf.solution.simpleByAliasTable.sampleAliasTable;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PartGetNeighborAliasTableParam extends PartitionGetParam {

  private int[] nodeIds;

  private int[] count;

  private int startIndex;

  private int endIndex;

  public PartGetNeighborAliasTableParam(int matrixId, PartitionKey part, int[] count, int[] nodeIds
      , int startIndex, int endIndex) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.count = count;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartGetNeighborAliasTableParam() {
    this(0, null, null, null, 0, 0);
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(int[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public int[] getCount() {
    return count;
  }

  public void setCount(int[] count) {
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
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeInt(nodeIds[i]);
      buf.writeInt(count[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int num = buf.readInt();
    nodeIds = new int[num];
    count = new int[num];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readInt();
      count[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 8 * (endIndex - startIndex);
  }

}
