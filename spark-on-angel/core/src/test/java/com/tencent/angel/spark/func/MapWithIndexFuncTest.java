package com.tencent.angel.spark.func;

import io.netty.buffer.ByteBuf;

public class MapWithIndexFuncTest implements MapWithIndexFunc {

  private double multiplier;

  public MapWithIndexFuncTest(double multiplier) {
    this.multiplier = multiplier;
  }

  public MapWithIndexFuncTest() {
  }

  @Override
  public double call(int index, double value) {
    if (index == 0) {
      return value * value;
    } else {
      return value * value * multiplier;
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
