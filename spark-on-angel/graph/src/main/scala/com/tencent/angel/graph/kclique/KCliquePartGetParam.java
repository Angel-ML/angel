package com.tencent.angel.graph.kclique;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

/**
 * Partition Sample parameter of SampleNeighbor
 */
public class KCliquePartGetParam extends PartitionGetParam {

  private final int startIndex;
  private final int endIndex;
  /**
   * Node ids
   */
  private long[] nodeIds;
  /**
   * Sample number, if count <= 0, means return all neighbors
   */
  private int count;
  private int readRowId;

  public KCliquePartGetParam(int matrixId, PartitionKey part, int count, long[] nodeIds
          , int startIndex, int endIndex, int rowId) {
    super(matrixId, part);
    this.nodeIds = nodeIds;
    this.count = count;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.readRowId = rowId;
  }

  public KCliquePartGetParam() {
    this(0, null, 0, null, 0, 0, 0);
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

  public int getReadRowId() {
    return readRowId;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(count);
    buf.writeInt(readRowId);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      buf.writeLong(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    count = buf.readInt();
    readRowId = buf.readInt();
    nodeIds = new long[buf.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 4 + 4 + 8 * (endIndex - startIndex);
  }
}

