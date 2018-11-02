package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class CbowDotPartitionResult extends PartitionGetResult {
  float[] values;
  int length;
  ByteBuf buf;

  public CbowDotPartitionResult(float[] values) {
    this.values = values;
  }

  public CbowDotPartitionResult() {}

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(values.length);
    for (int i = 0; i < values.length; i ++) buf.writeFloat(values[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.length = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();
  }

  @Override
  public int bufferLen() {
    return 4 + values.length * 4;
  }

  public void merge(float[] result) {
    for (int c = 0; c < result.length; c ++) result[c] += buf.readFloat();
  }

  public void clear() {
    buf.release();
  }
}