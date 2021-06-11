package com.tencent.angel.graph.common.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LongNeighbor implements IElement {

  private long[] nodeIds;

  public LongNeighbor(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public LongNeighbor() {
    this(null);
  }

  @Override
  public Object deepClone() {
    long[] newNodeIds = new long[nodeIds.length];
    System.arraycopy(nodeIds, 0, newNodeIds, 0, nodeIds.length);
    return new LongNeighbor(newNodeIds);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(nodeIds);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(nodeIds.length);
    for (int i = 0; i < nodeIds.length; i++) {
      output.writeLong(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    nodeIds = new long[input.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = input.readLong();
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }
}
