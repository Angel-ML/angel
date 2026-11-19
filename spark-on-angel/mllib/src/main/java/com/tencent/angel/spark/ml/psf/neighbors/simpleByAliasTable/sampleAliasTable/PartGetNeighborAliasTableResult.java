package com.tencent.angel.spark.ml.psf.neighbors.simpleByAliasTable.sampleAliasTable;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 *
 */
public class PartGetNeighborAliasTableResult extends PartitionGetResult {

  private int partId;
  private long[][] nodeIdToNeighbors;

  public PartGetNeighborAliasTableResult(int partitionId, long[][] nodeIdToNeighbors) {
    this.partId = partitionId;
    this.nodeIdToNeighbors = nodeIdToNeighbors;
    assert (nodeIdToNeighbors != null);
  }

  public PartGetNeighborAliasTableResult() {
    this(-1, null);
  }

  public long[][] getNodeIdToNeighbors() {
    return nodeIdToNeighbors;
  }

  public void setNodeIdToNeighbors(
      long[][] nodeIdToNeighbors) {
    this.nodeIdToNeighbors = nodeIdToNeighbors;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(nodeIdToNeighbors.length);
    for (int i = 0; i < nodeIdToNeighbors.length; i++) {
      if (nodeIdToNeighbors[i] == null) {
        output.writeInt(0);
      } else {
        output.writeInt(nodeIdToNeighbors[i].length);
        for (long value : nodeIdToNeighbors[i]) {
          output.writeLong(value);
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int size = input.readInt();
    nodeIdToNeighbors = new long[size][];

    for (int i = 0; i < size; i++) {
      long[] neighbors = new long[input.readInt()];
      for (int j = 0; j < neighbors.length; j++) {
        neighbors[j] = input.readLong();
      }
      nodeIdToNeighbors[i] = neighbors;
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < nodeIdToNeighbors.length; i++) {
      if (nodeIdToNeighbors[i] == null) {
        len += 4;
      } else {
        len += 4;
        len += 8 * nodeIdToNeighbors[i].length;
      }
    }
    return len;
  }
}
