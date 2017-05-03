package com.tencent.angel.spark.func;

import io.netty.buffer.ByteBuf;


public class Zip2MapWithIndexFuncTest implements Zip2MapWithIndexFunc {

  private double multiplier;

  public Zip2MapWithIndexFuncTest(double multiplier) {
    this.multiplier = multiplier;
  }

  public Zip2MapWithIndexFuncTest() {
  }

  @Override
  public double call(int index, double value1, double value2) {
    if (index == 0) {
      return value1 + value2 * value2;
    } else {
      return value1 * multiplier + value2 * value2;
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
