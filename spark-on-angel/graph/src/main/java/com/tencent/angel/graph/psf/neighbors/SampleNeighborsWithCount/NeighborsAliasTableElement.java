package com.tencent.angel.graph.psf.neighbors.SampleNeighborsWithCount;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (neighbors, accept, alias)
 */
public class NeighborsAliasTableElement implements IElement {

  private long[] neighborIds;
  private float[] accept;
  private int[] alias;


  public NeighborsAliasTableElement() {
    this(null, null, null);
  }

  public NeighborsAliasTableElement(long[] neighborIds, float[] accept, int[] alias) {
    assert (neighborIds.length == accept.length && neighborIds.length == alias.length);
    this.neighborIds = neighborIds;
    this.alias = alias;
    this.accept = accept;
  }

  public long[] getNeighborIds() {
    return neighborIds;
  }

  public float[] getAccept() {
    return accept;
  }

  public int[] getAlias() {
    return alias;
  }

  public int getNodesNum() {
    return neighborIds.length;
  }

  @Override
  public Object deepClone() {
    int len = neighborIds.length;
    long[] newNodeIds = new long[len];
    int[] newAlias = new int[len];
    float[] newAccept = new float[len];

    System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
    System.arraycopy(accept, 0, newAccept, 0, len);
    System.arraycopy(alias, 0, newAlias, 0, len);
    return new NeighborsAliasTableElement(newNodeIds, newAccept, newAlias);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, neighborIds);
    ByteBufSerdeUtils.serializeFloats(output, accept);
    ByteBufSerdeUtils.serializeInts(output, alias);
  }

  @Override
  public void deserialize(ByteBuf input) {
    neighborIds = ByteBufSerdeUtils.deserializeLongs(input);
    accept = ByteBufSerdeUtils.deserializeFloats(input);
    alias = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeLongs(output, neighborIds);
    StreamSerdeUtils.serializeFloats(output, accept);
    StreamSerdeUtils.serializeInts(output, alias);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    neighborIds = StreamSerdeUtils.deserializeLongs(input);
    accept = StreamSerdeUtils.deserializeFloats(input);
    alias = StreamSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(neighborIds)
        + ByteBufSerdeUtils.serializedFloatsLen(accept)
        + ByteBufSerdeUtils.serializedIntsLen(alias);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
