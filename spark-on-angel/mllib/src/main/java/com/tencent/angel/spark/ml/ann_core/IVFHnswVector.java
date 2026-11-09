package com.tencent.angel.spark.ml.ann_core;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IVFHnswVector implements IElement {

    private long[] label;
    private float[][] vector;

  public IVFHnswVector(long[] label, float[][] vector) {
    this.vector = vector;
    this.label = label;
  }

  public IVFHnswVector() {
    this(null, null);
  }

  public float[][] getVector() {
    return this.vector;
  }

  public void setVector(float[][] vector) {
    this.vector = vector;
  }

  public long[] getLabel() {
    return label;
  }

  public void setLabel(long[] label) {
    this.label = label;
  }


  @Override
  public Object deepClone() {
    long[] cloneLabel = new long[label.length];
    System.arraycopy(label, 0, cloneLabel, 0, label.length);

    float[][] cloneVector = new float[vector.length][];
    for (int i = 0; i < cloneVector.length; i++) {
      float[] aVector = vector[i];
      int aLength = aVector.length;
      cloneVector[i] = new float[aLength];
      System.arraycopy(aVector, 0, cloneVector[i], 0, aLength);
    }
    return new IVFHnswVector(cloneLabel, cloneVector);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, label);
    ByteBufSerdeUtils.serialize2DFloats(output, vector);
  }

  @Override
  public void deserialize(ByteBuf input) {
    label = ByteBufSerdeUtils.deserializeLongs(input);
    vector = ByteBufSerdeUtils.deserialize2DFloats(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(label)
        + ByteBufSerdeUtils.serialized2DFloatsLen(vector);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
      StreamSerdeUtils.serializeLongs(output, label);
      StreamSerdeUtils.serialize2DFloats(output, vector);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
      label = StreamSerdeUtils.deserializeLongs(input);
      vector = StreamSerdeUtils.deserialize2DFloats(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
