package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class DotPartitionParam extends PartitionGetParam {

  int seed;
  int partitionId;
  private byte[] bufData;
  ByteBuf edgeBuf;
  private int bufLength;

  public DotPartitionParam(int matrixId,
                           PartitionKey pkey,
                           byte[] bufData,
                           int bufLength) {
    super(matrixId, pkey);
    this.bufData = bufData;
    this.bufLength = bufLength;
  }

  public DotPartitionParam() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBytes(bufData);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.seed = buf.readInt();
    this.partitionId = buf.readInt();
    this.edgeBuf = buf;
    buf.retain();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + this.bufLength;
  }

  public void clear() {
    edgeBuf.release();
  }
}
