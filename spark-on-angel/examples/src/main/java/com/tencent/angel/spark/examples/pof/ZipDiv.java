package com.tencent.angel.spark.examples.pof;

import com.tencent.angel.spark.func.Zip2MapFunc;
import io.netty.buffer.ByteBuf;

public class ZipDiv implements Zip2MapFunc {

  public ZipDiv() {}

  @Override
  public double call(double value1, double value2) {
    return value1 / value2;
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
