package com.tencent.angel.spark.examples.pof;

import com.tencent.angel.spark.func.MapFunc;
import io.netty.buffer.ByteBuf;

public class MulScalar implements MapFunc {

  private double multiplier;

  public MulScalar(double multiplier) {
    this.multiplier = multiplier;
  }

  public MulScalar() {
  }

  @Override
  public double call(double value) {
    return value * multiplier;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(multiplier);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    multiplier = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }

}
