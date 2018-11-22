package com.tencent.angel.spark.ml.psf.embedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitPartitionParam extends PartitionUpdateParam {

  int numPartitions;
  int maxIndex;
  int maxLength;
  int negative;
  int order;
  int partDim;
  int window;


  public InitPartitionParam(int matrixId,
                            PartitionKey partKey,
                            int numPartitions,
                            int maxIndex,
                            int maxLength,
                            int negative,
                            int order,
                            int partDim,
                            int window) {
    super(matrixId, partKey);

    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.maxLength = maxLength;
    this.negative = negative;
    this.order = order;
    this.partDim = partDim;
    this.window = window;
  }

  public InitPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);

    buf.writeInt(numPartitions);
    buf.writeInt(maxIndex);
    buf.writeInt(maxLength);
    buf.writeInt(negative);
    buf.writeInt(order);
    buf.writeInt(partDim);
    buf.writeInt(window);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    numPartitions = buf.readInt();
    maxIndex = buf.readInt();
    maxLength = buf.readInt();
    negative = buf.readInt();
    order = buf.readInt();
    partDim = buf.readInt();
    window = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 28;
  }
}
