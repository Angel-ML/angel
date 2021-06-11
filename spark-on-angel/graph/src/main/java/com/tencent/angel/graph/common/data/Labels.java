package com.tencent.angel.graph.common.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Labels implements IElement {
  private float[] labels;

  public Labels(float[] labels) {
    this.labels = labels;
  }

  public Labels() {
    this(null);
  }

  @Override
  public Object deepClone() {
    return labels;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeFloats(output, labels);
  }

  @Override
  public void deserialize(ByteBuf input) {
    labels = ByteBufSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedFloatsLen(labels);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeFloats(output, labels);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    labels = StreamSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public float[] getWeights() {
    return labels;
  }
}
