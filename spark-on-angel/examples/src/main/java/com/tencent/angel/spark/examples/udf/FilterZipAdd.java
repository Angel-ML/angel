package com.tencent.angel.spark.examples.udf;

import com.tencent.angel.spark.func.Zip2MapWithIndexFunc;
import io.netty.buffer.ByteBuf;

public class FilterZipAdd implements Zip2MapWithIndexFunc {

  private int zeroIndex;

  public FilterZipAdd(int zeroIndex) {
    this.zeroIndex = zeroIndex;
  }

  public FilterZipAdd() {}

  @Override
  public double call(int index, double value1, double value2) {
    double result = 0;
    if (index != zeroIndex) {
      result = value1 + value2;
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
