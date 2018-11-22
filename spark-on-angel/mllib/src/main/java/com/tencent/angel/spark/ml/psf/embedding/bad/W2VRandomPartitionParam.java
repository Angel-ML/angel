package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class W2VRandomPartitionParam extends PartitionUpdateParam {

  int dimension;

  public W2VRandomPartitionParam(int matrixId, PartitionKey partKey,
                                 int dimension) {
    super(matrixId, partKey);
    this.dimension = dimension;
  }

  public W2VRandomPartitionParam() { }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(dimension);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    dimension = buf.readInt();
  }
}
