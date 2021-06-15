package com.tencent.angel.graph.model.neighbor.complex;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (neighbors, tags, attrs)
 */
public class NeighborsAttrTagElement implements IElement {

  private long[] neighborIds;
  private byte[] tags;
  private float[] attrs;

  public NeighborsAttrTagElement() {
    this(null, null, null);
  }

  public NeighborsAttrTagElement(long[] neighborIds, byte[] tags, float[] attrs) {
    assert (neighborIds.length == attrs.length && neighborIds.length == tags.length
        && tags.length == attrs.length);
    this.neighborIds = neighborIds;
    this.tags = tags;
    this.attrs = attrs;
  }

  public long[] getNeighborIds() {
    return neighborIds;
  }

  public byte[] getTags() {
    return tags;
  }

  public float[] getAttrs() {
    return attrs;
  }

  public int getNodesNum() {
    return neighborIds.length;
  }

  @Override
  public Object deepClone() {
    int len = neighborIds.length;
    long[] newNodeIds = new long[len];
    byte[] newTags = new byte[len];
    float[] newAttrs = new float[len];

    System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
    System.arraycopy(attrs, 0, newAttrs, 0, len);
    System.arraycopy(tags, 0, newTags, 0, len);
    return new NeighborsAttrTagElement(newNodeIds, newTags, newAttrs);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, neighborIds);
    ByteBufSerdeUtils.serializeBytes(output, tags);
    ByteBufSerdeUtils.serializeFloats(output, attrs);
  }

  @Override
  public void deserialize(ByteBuf input) {
    neighborIds = ByteBufSerdeUtils.deserializeLongs(input);
    tags = ByteBufSerdeUtils.deserializeBytes(input);
    attrs = ByteBufSerdeUtils.deserializeFloats(input);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeLongs(output, neighborIds);
    StreamSerdeUtils.serializeBytes(output, tags);
    StreamSerdeUtils.serializeFloats(output, attrs);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    neighborIds = StreamSerdeUtils.deserializeLongs(input);
    tags = StreamSerdeUtils.deserializeBytes(input);
    attrs = StreamSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(neighborIds)
        + ByteBufSerdeUtils.serializedBytesLen(tags)
        + ByteBufSerdeUtils.serializedFloatsLen(attrs);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
