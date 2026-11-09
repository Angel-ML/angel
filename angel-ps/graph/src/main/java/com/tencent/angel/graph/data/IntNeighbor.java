package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class IntNeighbor extends Neighbor {

  private int[] nodeIds;

  public IntNeighbor(int[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public IntNeighbor() {
    this(null);
  }

  @Override
  public Neighbor deepClone() {
    int[] newNodeIds = new int[nodeIds.length];
    System.arraycopy(nodeIds, 0, newNodeIds, 0, nodeIds.length);
    return new IntNeighbor(newNodeIds);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInts(output, nodeIds);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedIntsLen(nodeIds);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(nodeIds.length);
    for (int i = 0; i < nodeIds.length; i++) {
      output.writeInt(nodeIds[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    nodeIds = new int[input.readInt()];
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIds[i] = input.readInt();
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(int[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  @Override
  public NeighborStorageType getStorageType() {
    return NeighborStorageType.INTARRAY;
  }

  @Override
  public int getNeighborsNum() {
    return nodeIds.length;
  }


  public Neighbor sampleNeighbors(int count, Boolean shuffle, long seed) {
    Random r = new Random(seed);
    int len = getNeighborsNum();
    int[] sampleNeighbors;

    if (count <= 0 || len <= count) {
      sampleNeighbors = nodeIds;
    } else {
      sampleNeighbors = new int[count];
      if (shuffle) IntArrays.shuffle(nodeIds, r);
      // If the neighbor number > count, just copy a range of neighbors to
      // the result array, the copy position is random

      int startPos = r.nextInt(len);
      if (startPos + count <= len) {
        System.arraycopy(nodeIds, startPos,
            sampleNeighbors, 0, count);
      } else {
        System.arraycopy(nodeIds, startPos, sampleNeighbors, 0,
            len - startPos);
        System.arraycopy(nodeIds, 0, sampleNeighbors,
            len - startPos,
            count - (len - startPos));
      }
    }

    return new IntNeighbor(sampleNeighbors);
  }

  public Neighbor sampleNeighbors(int count, int startPos) {
    int len = getNeighborsNum();
    int[] sampleNeighbors;

    if (count <= 0 || len <= count) {
      sampleNeighbors = nodeIds;
    } else {
      sampleNeighbors = new int[count];
      // If the neighbor number > count, just copy a range of neighbors to
      // the result array, the copy position is random

      if (startPos + count <= len) {
        System.arraycopy(nodeIds, startPos,
            sampleNeighbors, 0, count);
      } else {
        System.arraycopy(nodeIds, startPos, sampleNeighbors, 0,
            len - startPos);
        System.arraycopy(nodeIds, 0, sampleNeighbors,
            len - startPos,
            count - (len - startPos));
      }
    }

    return new IntNeighbor(sampleNeighbors);
  }
}
