package com.tencent.angel.spark.ml.psf.neighbors;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 *
 */
public class PartGetNeighborAttrByteResult extends PartitionGetResult {

  private int partId;
  private NeighborsAttrTagElement[] neighbors;

  public PartGetNeighborAttrByteResult(int partitionId, NeighborsAttrTagElement[] neighbors) {
    this.partId = partitionId;
    this.neighbors = neighbors;
    assert (neighbors != null);
  }

  public PartGetNeighborAttrByteResult() {
    this(-1, null);
  }

  public NeighborsAttrTagElement[] getNeighbors() {
    return neighbors;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(neighbors.length);
    for (int i = 0; i < neighbors.length; i++) {
      if (neighbors[i] == null) {
        output.writeInt(0);
        output.writeInt(0);
      } else {
        neighbors[i].serialize(output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int len = input.readInt();
    neighbors = new NeighborsAttrTagElement[len];

    for (int i = 0; i < len; i++) {
      NeighborsAttrTagElement elem = new NeighborsAttrTagElement();
      elem.deserialize(input);
      neighbors[i] = elem;
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < neighbors.length; i++) {
      if (neighbors[i] == null) {
        len += 4 * 2;
      } else {
        len += neighbors[i].bufferLen();
      }
    }
    return len;
  }
}
