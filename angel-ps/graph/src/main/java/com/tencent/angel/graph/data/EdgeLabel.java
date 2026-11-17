package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EdgeLabel implements IElement {

  private long[] nodeIds;
  private int[] labels;

  public EdgeLabel(long[] nodeIds, int[] labels) {
    this.nodeIds = nodeIds;
    this.labels = labels;
  }

  public EdgeLabel() {
    this(null, null);
  }

  @Override
  public Object deepClone() {
    long[] newNodeIds = new long[nodeIds.length];
    System.arraycopy(nodeIds, 0, newNodeIds, 0, nodeIds.length);
    int[] newLabels = new int[nodeIds.length];
    System.arraycopy(labels, 0, newLabels, 0, labels.length);
    return new EdgeLabel(newNodeIds, newLabels);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
    ByteBufSerdeUtils.serializeInts(output, labels);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
    labels = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(nodeIds) + ByteBufSerdeUtils.serializedIntsLen(labels);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(nodeIds.length);
    for (int i = 0; i < nodeIds.length; i++) {
      output.writeLong(nodeIds[i]);
    }
    output.writeInt(labels.length);
    for (int i = 0; i < labels.length; i++) {
      output.writeInt(labels[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    nodeIds = new long[input.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = input.readLong();
    }
    labels = new int[input.readInt()];
    for (int i = 0; i < labels.length; i++) {
      labels[i] = input.readInt();
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

  public int[] getLabels() {
    return labels;
  }

  public void setLabels(int[] labels) {
    this.labels = labels;
  }
}
