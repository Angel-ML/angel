package com.tencent.angel.spark.examples.ml.udf;

import com.tencent.angel.spark.func.Zip2MapFunc;
import io.netty.buffer.ByteBuf;

public class CorrectDirection implements Zip2MapFunc {

  public CorrectDirection() {}

  @Override
  public double call(double d, double g) {
    if (d * g < 0) {
      return d;
    } else {
      return 0;
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
