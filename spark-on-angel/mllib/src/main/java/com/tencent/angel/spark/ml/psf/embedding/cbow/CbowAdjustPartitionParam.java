package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class CbowAdjustPartitionParam extends PartitionUpdateParam {

  int seed;
  int negative;
  int window;
  int partDim;
  int partitionId;
  private float[] gradient;
  ByteBuf buf;

  public CbowAdjustPartitionParam(int matrixId,
                                  PartitionKey partKey,
                                  int seed,
                                  int negative,
                                  int window,
                                  int partDim,
                                  int partitionId,
                                  float[] gradient) {
    super(matrixId, partKey);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
    this.gradient = gradient;
  }

  public CbowAdjustPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(seed);
    buf.writeInt(negative);
    buf.writeInt(window);
    buf.writeInt(partDim);
    buf.writeInt(partitionId);
    buf.writeInt(gradient.length);
    for (int a = 0; a < gradient.length; a++) buf.writeFloat(gradient[a]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    seed = buf.readInt();
    negative = buf.readInt();
    window = buf.readInt();
    partDim = buf.readInt();
    partitionId = buf.readInt();
    this.buf = buf;
    buf.retain();
  }

  public void clear() {
    buf.release();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 24 + 4 + gradient.length * 4;
  }
}
