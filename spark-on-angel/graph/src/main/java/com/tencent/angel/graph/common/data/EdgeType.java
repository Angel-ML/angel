package com.tencent.angel.graph.common.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EdgeType implements IElement {
  private int[] types;

  public EdgeType(int[] types) {
    this.types = types;
  }

  public EdgeType() {
    this(null);
  }

  @Override
  public Object deepClone() {
    return types;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInts(output, types);
  }

  @Override
  public void deserialize(ByteBuf input) {
    types = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedIntsLen(types);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeInts(output, types);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    types = StreamSerdeUtils.deserializeInts(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public int[] getTypes() {
    return types;
  }
}
