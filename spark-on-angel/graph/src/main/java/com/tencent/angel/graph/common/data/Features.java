package com.tencent.angel.graph.common.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Features implements IElement {

  private IntFloatVector[] features;

  public Features(IntFloatVector[] features) {
    this.features = features;
  }

  public Features() {
    this(null);
  }

  @Override
  public Object deepClone() {
    return features;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeIntFloatVectors(output, features);
  }

  @Override
  public void deserialize(ByteBuf input) {
    features = ByteBufSerdeUtils.deserializeIntFloatVectors(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedIntFloatVectorsLen(features);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeIntFloatVectors(output, features);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    features = StreamSerdeUtils.deserializeIntFloatVectors(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public IntFloatVector[] getFeatures() {
    return features;
  }
}
