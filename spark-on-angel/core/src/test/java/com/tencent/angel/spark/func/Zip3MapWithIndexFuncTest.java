package com.tencent.angel.spark.func;

import io.netty.buffer.ByteBuf;


public class Zip3MapWithIndexFuncTest implements Zip3MapWithIndexFunc {

  private double multiplier;

  public Zip3MapWithIndexFuncTest(double multiplier) {
    this.multiplier = multiplier;
  }

  public Zip3MapWithIndexFuncTest() {
  }

  @Override
  public double call(int index, double value1, double value2, double value3) {
    if (index == 0) {
      return value1 * (1.0 - value3) + value2 * value2;
    } else {
      return multiplier * value1 * (1.0 - value3) + value2 * value2;
    }
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
