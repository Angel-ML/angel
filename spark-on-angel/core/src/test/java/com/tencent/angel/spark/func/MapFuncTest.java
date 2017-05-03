package com.tencent.angel.spark.func;

import io.netty.buffer.ByteBuf;

public class MapFuncTest implements MapFunc {

  private double multiplier;

  public MapFuncTest(double multiplier) {
    this.multiplier = multiplier;
  }

  public MapFuncTest() {
  }

  @Override
  public double call(double value) {
    return value * value * multiplier;
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
