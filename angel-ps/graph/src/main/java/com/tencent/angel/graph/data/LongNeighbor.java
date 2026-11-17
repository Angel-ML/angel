package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.LongArrays;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

public class LongNeighbor extends Neighbor {

  private long[] nodeIds;

  public LongNeighbor(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public LongNeighbor() {
    this(null);
  }

  @Override
  public Neighbor deepClone() {
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

  @Override
  public NeighborStorageType getStorageType() {
    return NeighborStorageType.LONGARRAY;
  }

  @Override
  public int getNeighborsNum() {
    return nodeIds.length;
  }

  public Neighbor sampleNeighbors(int count, Boolean shuffle, long seed) {
    Random r = new Random(seed);
    int len = getNeighborsNum();
    long[] sampleNeighbors;

    if (count <= 0 || len <= count) {
      sampleNeighbors = nodeIds;
    } else {
      sampleNeighbors = new long[count];
      if (shuffle) LongArrays.shuffle(nodeIds, r);
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

    return new LongNeighbor(sampleNeighbors);
  }

  public Neighbor sampleNeighbors(int count, int startPos) {
    int len = getNeighborsNum();
    long[] sampleNeighbors;

    if (count <= 0 || len <= count) {
      sampleNeighbors = nodeIds;
    } else {
      sampleNeighbors = new long[count];
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

    return new LongNeighbor(sampleNeighbors);
  }
}
