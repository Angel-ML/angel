package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EmbeddingOrGradWithField extends EmbeddingOrGrad {

  private int fieldId;

  public EmbeddingOrGradWithField(float[] values, int fieldId) {
    super(values);
    this.fieldId = fieldId;
  }

  public EmbeddingOrGradWithField() {
    this(null, -1);
  }

  public int getFieldId() {
    return fieldId;
  }

  public void setFieldId(int fieldId) {
    this.fieldId = fieldId;
  }

  @Override
  public Object deepClone() {
    float[] cloneValues = new float[getValues().length];
    System.arraycopy(getValues(), 0, cloneValues, 0, getValues().length);
    return new EmbeddingOrGradWithField(cloneValues, fieldId);
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    ByteBufSerdeUtils.serializeInt(output, fieldId);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    fieldId = ByteBufSerdeUtils.deserializeInt(input);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + ByteBufSerdeUtils.INT_LENGTH;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    super.serialize(output);
    StreamSerdeUtils.serializeInt(output, fieldId);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    super.deserialize(input);
    fieldId = StreamSerdeUtils.deserializeInt(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
