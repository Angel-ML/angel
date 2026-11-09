package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class FieldUpdateInfo implements IElement {

  private int fieldId;
  private long updateTimestampSecond;

  public FieldUpdateInfo(int fieldId, long updateTimestampSecond) {
    this.fieldId = fieldId;
    this.updateTimestampSecond = updateTimestampSecond;
  }

  public FieldUpdateInfo() {
    this(-1, -1L);
  }

  public int getFieldId() {
    return fieldId;
  }

  public void setFieldId(int fieldId) {
    this.fieldId = fieldId;
  }

  public long getUpdateTimestampSecond() {
    return updateTimestampSecond;
  }

  public void setUpdateTimestampSecond(long updateTimestampSecond) {
    this.updateTimestampSecond = updateTimestampSecond;
  }

  @Override
  public Object deepClone() {
    return new FieldUpdateInfo(fieldId, updateTimestampSecond);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, fieldId);
    ByteBufSerdeUtils.serializeLong(output, updateTimestampSecond);
  }

  @Override
  public void deserialize(ByteBuf input) {
    fieldId = ByteBufSerdeUtils.deserializeInt(input);
    updateTimestampSecond = ByteBufSerdeUtils.deserializeLong(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.LONG_LENGTH;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeInt(output, fieldId);
    StreamSerdeUtils.serializeLong(output, updateTimestampSecond);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    fieldId = StreamSerdeUtils.deserializeInt(input);
    updateTimestampSecond = StreamSerdeUtils.deserializeLong(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
