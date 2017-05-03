package com.tencent.angel.spark.examples.ml.udf;

import com.tencent.angel.spark.func.Zip2MapFunc;
import io.netty.buffer.ByteBuf;

public class ComputeOrthant implements Zip2MapFunc {

  public ComputeOrthant() {}

  @Override
  public double call(double v, double gv) {
    if (v != 0) {
      return Math.signum(v);
    } else {
      return Math.signum(-gv);
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
