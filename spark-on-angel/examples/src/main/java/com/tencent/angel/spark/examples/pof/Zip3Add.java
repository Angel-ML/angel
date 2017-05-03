package com.tencent.angel.spark.examples.pof;

import com.tencent.angel.spark.func.Zip3MapFunc;
import io.netty.buffer.ByteBuf;

public class Zip3Add implements Zip3MapFunc {

  public Zip3Add() {}

  @Override
  public double call(double value1, double value2, double value3) {
    return value1 + value2 + value3;
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
