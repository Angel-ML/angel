package com.tencent.angel.spark.ml.psf.solution.simpleByAliasTable.sampleAliasTable;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 *
 */
public class PartGetNeighborAliasTableResult extends PartitionGetResult {

  private int partId;
  private int[][] nodeIdToNeighbors;

  public PartGetNeighborAliasTableResult(int partitionId, int[][] nodeIdToNeighbors) {
    this.partId = partitionId;
    this.nodeIdToNeighbors = nodeIdToNeighbors;
    assert (nodeIdToNeighbors != null);
  }

  public PartGetNeighborAliasTableResult() {
    this(-1, null);
  }

  public int[][] getNodeIdToNeighbors() {
    return nodeIdToNeighbors;
  }

  public void setNodeIdToNeighbors(
      int[][] nodeIdToNeighbors) {
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
        for (int value : nodeIdToNeighbors[i]) {
          output.writeInt(value);
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int size = input.readInt();
    nodeIdToNeighbors = new int[size][];

    for (int i = 0; i < size; i++) {
      int[] neighbors = new int[input.readInt()];
      for (int j = 0; j < neighbors.length; j++) {
        neighbors[j] = input.readInt();
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
        len += 4 * nodeIdToNeighbors[i].length;
      }
    }
    return len;
  }
}
