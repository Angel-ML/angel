package com.tencent.angel.spark.func;

import io.netty.buffer.ByteBuf;

public class Zip3MapFuncTest implements Zip3MapFunc {

  private double multiplier;

  public Zip3MapFuncTest(double multiplier) {
    this.multiplier = multiplier;
  }

  public Zip3MapFuncTest() {
  }

  @Override
  public double call(double value1, double value2, double value3) {
    return multiplier * value1 * (1.0 - value3) + value2 * value2;
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
