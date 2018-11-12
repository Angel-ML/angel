package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class AdjustPartitionParam extends PartitionUpdateParam {

  int seed;
  int partitionId;
  int batchSize;
  private int negative;
  private byte[] edgesAndGrads;
  ByteBuf dataBuf;

  public AdjustPartitionParam(int matrixId,
                              PartitionKey partKey,
                              int seed,
                              int negative,
                              int partitionId,
                              int batchSize,
                              byte[] edgesAndGrads) {
    super(matrixId, partKey);
    this.seed = seed;
    this.partitionId = partitionId;
    this.batchSize = batchSize;
    this.edgesAndGrads = edgesAndGrads;
    this.negative = negative;
  }

  public AdjustPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(seed);
    buf.writeInt(partitionId);
    buf.writeInt(batchSize);
    buf.writeBytes(edgesAndGrads);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    seed = buf.readInt();
    partitionId = buf.readInt();
    batchSize = buf.readInt();
    dataBuf = buf;
    dataBuf.retain();
  }

  public void clear() {
    dataBuf.release();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 24 + batchSize * (4 * negative + 12);
  }
}
