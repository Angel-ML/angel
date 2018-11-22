package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class W2VPullPartitionResult extends PartitionGetResult {

  int start;
  int length;
  int dimension;
  float[] layers;
  ByteBuf buf;

  public W2VPullPartitionResult(int start, int dimension, float[] layers) {
    this.start = start;
    this.layers = layers;
    this.dimension = dimension;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(start);
    buf.writeInt(dimension);
    buf.writeInt(layers.length);
    for (int a = 0; a < layers.length; a ++) buf.writeFloat(layers[a]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    start = buf.readInt();
    dimension = buf.readInt();
    length = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();

  }

  public void merge(float[] results, int start) {
    for (int a = 0; a < length; a ++)
      results[a + start] = buf.readFloat();
  }

  public void clear() {
    this.buf.release();
  }

  @Override
  public int bufferLen() {
    return 4 + 4 + layers.length * 4;
  }
}
