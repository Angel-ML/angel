package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;
import io.netty.buffer.ByteBuf;

public class ScalarPartitionAggrResult extends PartitionAggrResult {
  double result;

  public ScalarPartitionAggrResult(double result) {
    this.result = result;
  }

  public ScalarPartitionAggrResult() {}

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(result);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    result = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
