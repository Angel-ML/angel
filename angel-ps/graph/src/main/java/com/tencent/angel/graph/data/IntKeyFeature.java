package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IntKeyFeature extends Feature {
  int[] indexes;

  public IntKeyFeature() {
    this(null, null, null, null);
  }

  public IntKeyFeature(float[] dense, short[] fields, float[] values, int[] indexes) {
    super(dense, fields, values, FeatureKeyType.INT);
    this.indexes = indexes;
  }

  public IntKeyFeature(float[] dense, short[] fields, int[] indexes) {
    this(dense, fields, null, indexes);
  }

  public IntKeyFeature(short[] fields, float[] values, int[] indexes) {
    this(null, fields, values, indexes);
  }

  public IntKeyFeature(short[] fields, int[] indexes) {
    this(fields, null, indexes);
  }

  @Override
  public FeatureKeyType getKeyType() {
    return FeatureKeyType.INT;
  }

  public int[] getIndexes() {
    return indexes;
  }

  public void setIndexes(int[] indexes) {
    this.indexes = indexes;
  }

  @Override
  public Object deepClone() {
    float[] denseClone = null;
    if (dense != null) {
      int len = dense.length;
      denseClone = new float[dense.length];
      System.arraycopy(dense, 0, denseClone, 0, len);
    }

    short[] fieldsClone = null;
    if (fields != null) {
      int len = fields.length;
      fieldsClone = new short[len];
      System.arraycopy(fields, 0, fieldsClone, 0, len);
    }

    float[] valuesClone = null;
    if (values != null) {
      int len = values.length;
      valuesClone = new float[values.length];
      System.arraycopy(values, 0, valuesClone, 0, len);
    }

    int[] indexesClone = null;
    if (indexes != null) {
      int len = indexes.length;
      indexesClone = new int[len];
      System.arraycopy(indexes, 0, indexesClone, 0, len);
    }

    return new IntKeyFeature(denseClone, fieldsClone, valuesClone, indexesClone);
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInts(output, indexes);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    indexes = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedIntsLen(indexes);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    super.serialize(output);
    StreamSerdeUtils.serializeInts(output, indexes);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    super.deserialize(input);
    indexes = StreamSerdeUtils.deserializeInts(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}