package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LongKeyFeature extends Feature {
  long[] indexes;

  public LongKeyFeature() {
    this(null, null, null, null);
  }

  public LongKeyFeature(float[] dense, short[] fields, float[] values, long[] indexes) {
    super(dense, fields, values, FeatureKeyType.LONG);
    this.indexes = indexes;
  }

  public LongKeyFeature(float[] dense, short[] fields, long[] indexes) {
   this(dense, fields, null, indexes);
  }

  public LongKeyFeature(short[] fields, float[] values, long[] indexes) {
    this(null, fields, values, indexes);
  }

  public LongKeyFeature(short[] fields, long[] indexes) {
    this(fields, null, indexes);
  }

  @Override
  public FeatureKeyType getKeyType() {
    return FeatureKeyType.LONG;
  }

  public long[] getIndexes() {
    return indexes;
  }

  public void setIndexes(long[] indexes) {
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

    long[] indexesClone = null;
    if (indexes != null) {
      int len = indexes.length;
      indexesClone = new long[len];
      System.arraycopy(indexes, 0, indexesClone, 0, len);
    }

    return new LongKeyFeature(denseClone, fieldsClone, valuesClone, indexesClone);
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeLongs(output, indexes);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    indexes = ByteBufSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.serializedLongsLen(indexes);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    super.serialize(output);
    StreamSerdeUtils.serializeLongs(output, indexes);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    super.deserialize(input);
    indexes = StreamSerdeUtils.deserializeLongs(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
