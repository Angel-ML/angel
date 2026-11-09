package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EdgeTimestamp implements IElement {
  private int[] timestamps;

  public EdgeTimestamp(int[] timestamps) {
    this.timestamps = timestamps;
  }

  public EdgeTimestamp() {
    this(null);
  }

  @Override
  public Object deepClone() {
    int[] cloneTimestamps = new int[timestamps.length];
    System.arraycopy(timestamps, 0, cloneTimestamps, 0, timestamps.length);
    return new EdgeTimestamp(cloneTimestamps);
  }

  @Override
  public void serialize(ByteBuf output) {

    ByteBufSerdeUtils.serializeInts(output, timestamps);
  }

  @Override
  public void deserialize(ByteBuf input) {
    timestamps = ByteBufSerdeUtils.deserializeInts(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedIntsLen(timestamps);
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeInts(output, timestamps);
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    timestamps = StreamSerdeUtils.deserializeInts(input);
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  public int[] getTimestamps() {
    return timestamps;
  }
}