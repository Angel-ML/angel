package com.tencent.angel.spark.examples.ml.udf;

import com.tencent.angel.spark.func.Zip3MapFunc;
import io.netty.buffer.ByteBuf;

public class Adjust implements Zip3MapFunc {

  public Adjust() {}

  @Override
  public double call(double xv, double v, double l1regValue) {
    if (l1regValue < 0)
      throw new IllegalArgumentException("negative l1reg value!");

    if (l1regValue == 0.0) {
      return v;
    } else if (xv == 0.0) {
      double deltaPlus = v + l1regValue;
      double deltaMinus = v - l1regValue;
      if (deltaMinus > 0)
        return deltaMinus;
      else if (deltaPlus < 0)
        return deltaPlus;
      else
        return 0.0;
    } else {
      return v + Math.signum(xv) * l1regValue;
    }
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
