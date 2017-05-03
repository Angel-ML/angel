package com.tencent.angel.spark.examples.ml.udf;

import com.tencent.angel.spark.func.Zip2MapFunc;
import io.netty.buffer.ByteBuf;

public class ComputeAdjustValue implements Zip2MapFunc {

  public ComputeAdjustValue() {}

  @Override
  public double call(double xv, double l1regValue) {
    if (l1regValue < 0)
      throw new IllegalArgumentException("negative l1reg value!");

    if (l1regValue == 0.0) {
      return 0.0;
    } else {
      return Math.abs(l1regValue * xv);
    }
  }

  @Override
  public void serialize(ByteBuf buf) {}

  @Override
  public void deserialize(ByteBuf buf) {}

  @Override
  public int bufferLen() {
    return 0;
  }

}
