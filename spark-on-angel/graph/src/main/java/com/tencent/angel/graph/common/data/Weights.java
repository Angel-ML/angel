package com.tencent.angel.graph.common.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Weights implements IElement {
  private float[] weights;

  public Weights(float[] weights) {
    this.weights = weights;
  }

  public Weights() {
    this(null);
  }

  @Override
  public Object deepClone() {
    return weights;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeFloats(output, weights);
  }

  @Override
  public void deserialize(ByteBuf input) {
    weights = ByteBufSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedFloatsLen(weights);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeFloats(output, weights);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    weights = StreamSerdeUtils.deserializeFloats(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public float[] getWeights() {
    return weights;
  }
}
