package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class CbowDotPartitionParam extends PartitionGetParam {

  int seed;
  int negative;
  int window;
  int partDim;
  int partitionId;

  public CbowDotPartitionParam(int matrixId,
                               int seed,
                               int negative,
                               int window,
                               int partDim,
                               int partitionId,
                               PartitionKey pkey) {
    super(matrixId, pkey);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
  }

  public CbowDotPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(seed);
    buf.writeInt(negative);
    buf.writeInt(window);
    buf.writeInt(partDim);
    buf.writeInt(partitionId);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.seed = buf.readInt();
    this.negative = buf.readInt();
    this.window = buf.readInt();
    this.partDim = buf.readInt();
    this.partitionId = buf.readInt();
  }
}
