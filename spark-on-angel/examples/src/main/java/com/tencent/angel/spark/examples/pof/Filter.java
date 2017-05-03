package com.tencent.angel.spark.examples.pof;

import com.tencent.angel.spark.func.MapWithIndexFunc;
import io.netty.buffer.ByteBuf;

public class Filter implements MapWithIndexFunc {

  private int zeroIndex;

  public Filter(int zeroIndex) {
    this.zeroIndex = zeroIndex;
  }

  public Filter() {
  }

  @Override
  public double call(int index, double value) {
    double result = 0;
    if (index != zeroIndex) {
      result = value;
    }
    return result;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(zeroIndex);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    zeroIndex = buf.readInt();
  }

  @Override
  public int bufferLen() {
    return 4;
  }

}
