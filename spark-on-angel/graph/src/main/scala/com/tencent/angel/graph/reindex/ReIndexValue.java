package com.tencent.angel.graph.reindex;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A user-define data type that store node information on PS
 */
public class ReIndexValue implements IElement {

  private long value;

  public ReIndexValue(long value) {
    this.value = value;
  }

  public ReIndexValue() {
    this(-1);
  }

  public long getValue() {
    return value;
  }

  public void setValue(long value) {
    this.value = value;
  }

  @Override
  public Object deepClone() {
    return new ReIndexValue(value);
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeLong(value);
  }

  @Override
  public void deserialize(ByteBuf input) {
    value = input.readInt();
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.LONG_LENGTH;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeLong(value);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    value = input.readLong();
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}