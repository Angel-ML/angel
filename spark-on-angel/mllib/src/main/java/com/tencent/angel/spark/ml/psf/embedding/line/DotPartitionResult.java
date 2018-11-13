package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class DotPartitionResult extends PartitionGetResult {
  float[] values;
  int length;
  ByteBuf buf;

  public DotPartitionResult(float[] values) {
    this.values = values;
  }

  public DotPartitionResult() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(values.length);
    for (float value : values) {
      buf.writeFloat(value);
    }
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
    for (int c = 0; c < result.length; c++) result[c] += buf.readFloat();
  }

  public void clear() {
    buf.release();
  }
}
