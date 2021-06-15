package com.tencent.angel.graph.common.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class Feature implements IElement {
  private IntFloatVector features;

  public Feature(IntFloatVector features) {
    this.features = features;
  }

  public Feature() {
    this(null);
  }

  @Override
  public Object deepClone() {
    // Just return original features
    return features;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeVector(output, features);
  }

  @Override
  public void deserialize(ByteBuf input) {
    features = (IntFloatVector) ByteBufSerdeUtils.deserializeVector(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedVectorLen(features);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeVector(output, features);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    features = (IntFloatVector) StreamSerdeUtils.deserializeVector(input);
  }

  @Override
  public int dataLen() {
    return StreamSerdeUtils.serializedVectorLen(features);
  }

  public IntFloatVector getFeatures() {
    return features;
  }

  public void setFeatures(IntFloatVector features) {
    this.features = features;
  }
}
