package com.tencent.angel.spark.func;

import io.netty.buffer.ByteBuf;

public class Zip2MapFuncTest implements Zip2MapFunc {

  private double multiplier;

  public Zip2MapFuncTest(double multiplier) {
    this.multiplier = multiplier;
  }

  public Zip2MapFuncTest() {
  }

  @Override
  public double call(double value1, double value2) {
    return multiplier * value1 + value2 * value2;
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
