package com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalk.sampleRandomNeighborTable;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (neighbors list)
 */
public class NeighborsRandomTableElement implements IElement {

  private long[] neighborIds;


  public NeighborsRandomTableElement() {
    this(null);
  }

  public NeighborsRandomTableElement(long[] neighborIds) {
    this.neighborIds = neighborIds;
  }

  public long[] getNeighborIds() {
    return neighborIds;
  }

  public int getNodesNum() {
    return neighborIds.length;
  }

  @Override
  public Object deepClone() {
    int len = neighborIds.length;
    long[] newNodeIds = new long[len];
    System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
    return new NeighborsRandomTableElement(newNodeIds);
  }

  @Override
  public void serialize(ByteBuf output) {
    int len = neighborIds.length;
    output.writeInt(len);
    for (int i = 0; i < len; i++) {
      output.writeLong(neighborIds[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    neighborIds = new long[len];
    for (int i = 0; i < len; i++) {
      neighborIds[i] = input.readLong();
    }
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    int len = neighborIds.length;
    output.writeInt(len);
    for (int i = 0; i < len; i++) {
      output.writeLong(neighborIds[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    int len = input.readInt();
    neighborIds = new long[len];
    for (int i = 0; i < len; i++) {
      neighborIds[i] = input.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return Integer.BYTES + neighborIds.length * Long.BYTES;
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
