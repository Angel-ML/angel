package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitPartitionParam extends PartitionUpdateParam {

  int numPartitions;
  int maxIndex;
  int maxLength;


  public InitPartitionParam(int matrixId,
                            PartitionKey partKey,
                            int numPartitions,
                            int maxIndex,
                            int maxLength) {
    super(matrixId, partKey);

    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.maxLength = maxLength;

  }

  public InitPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);

    buf.writeInt(numPartitions);
    buf.writeInt(maxIndex);
    buf.writeInt(maxLength);

  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    numPartitions = buf.readInt();
    maxIndex = buf.readInt();
    maxLength = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen();
  }
}
