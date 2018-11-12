package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class DotPartitionParam extends PartitionGetParam {

  int seed;
  int partitionId;
  int batchSize;
  private byte[] edges;
  ByteBuf edgeBuf;

  public DotPartitionParam(int matrixId,
                           int seed,
                           int partitionId,
                           PartitionKey pkey,
                           int batchSize,
                           byte[] edges) {
    super(matrixId, pkey);
    this.seed = seed;
    this.partitionId = partitionId;
    this.batchSize = batchSize;
    this.edges = edges;
  }

  public DotPartitionParam() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(seed);
    buf.writeInt(partitionId);
    buf.writeInt(batchSize);
    buf.writeBytes(edges);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.seed = buf.readInt();
    this.partitionId = buf.readInt();
    this.batchSize = buf.readInt();
    this.edgeBuf = buf;
    buf.retain();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 12 + batchSize * 8;
  }

  public void clear(){
    edgeBuf.release();
  }
}
