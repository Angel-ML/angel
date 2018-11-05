package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class CbowInitPartitionParam extends PartitionUpdateParam {

  int partitionId;
  int numPartitions;
  int maxIndex;
  int concurrentLevel;


  public CbowInitPartitionParam(int matrixId,
                                PartitionKey partKey,
                                int partitionId,
                                int numPartitions,
                                int maxIndex,
                                int concurrentLevel) {
    super(matrixId, partKey);
    this.partitionId = partitionId;
    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.concurrentLevel = concurrentLevel;

  }

  public CbowInitPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(partitionId);
    buf.writeInt(numPartitions);
    buf.writeInt(maxIndex);
    buf.writeInt(concurrentLevel);

  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    partitionId = buf.readInt();
    numPartitions = buf.readInt();
    maxIndex = buf.readInt();
    concurrentLevel = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen();
  }
}
