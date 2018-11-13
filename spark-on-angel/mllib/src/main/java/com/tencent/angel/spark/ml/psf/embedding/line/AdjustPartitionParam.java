package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class AdjustPartitionParam extends PartitionUpdateParam {

  int seed;
  int partitionId;
  private byte[] dataBuf;
  ByteBuf edgesAndGradsBuf;
  private int bufLength;

  public AdjustPartitionParam(int matrixId,
                              PartitionKey partKey,
                              byte[] dataBuf,
                              int bufLength) {
    super(matrixId, partKey);
    this.dataBuf = dataBuf;
    this.bufLength = bufLength;
  }

  public AdjustPartitionParam() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBytes(dataBuf);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    seed = buf.readInt();
    partitionId = buf.readInt();
    edgesAndGradsBuf = buf;
    edgesAndGradsBuf.retain();
  }

  public void clear() {
    edgesAndGradsBuf.release();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + this.bufLength;
  }
}
