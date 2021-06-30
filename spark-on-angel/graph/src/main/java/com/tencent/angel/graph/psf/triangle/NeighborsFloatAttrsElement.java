package com.tencent.angel.graph.psf.triangle;

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
public class NeighborsFloatAttrsElement implements IElement {

  private long[] neighborIds;
  private float[] attrs;

  public NeighborsFloatAttrsElement() {
    this(new long[0], new float[0]);
  }

  public NeighborsFloatAttrsElement(long[] neighborIds, float[] attrs) {
    assert (neighborIds.length == attrs.length);
    this.neighborIds = neighborIds;
    this.attrs = attrs;
  }

  public long[] getNeighborIds() {
    return neighborIds;
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
    float[] newAttrs = new float[len];

    System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
    System.arraycopy(attrs, 0, newAttrs, 0, len);
    return new NeighborsFloatAttrsElement(newNodeIds, newAttrs);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, neighborIds);
    ByteBufSerdeUtils.serializeFloats(output, attrs);
  }

  @Override
  public void deserialize(ByteBuf input) {
    neighborIds = ByteBufSerdeUtils.deserializeLongs(input);
    attrs = ByteBufSerdeUtils.deserializeFloats(input);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeLongs(output, neighborIds);
    StreamSerdeUtils.serializeFloats(output, attrs);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    neighborIds = StreamSerdeUtils.deserializeLongs(input);
    attrs = StreamSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(neighborIds)
        + ByteBufSerdeUtils.serializedFloatsLen(attrs);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
