package com.tencent.angel.spark.examples.ml.udf;

import com.tencent.angel.spark.func.Zip2MapFunc;
import io.netty.buffer.ByteBuf;

public class Project implements Zip2MapFunc {

  public Project() {}

  @Override
  public double call(double v, double ov) {
    if (Math.signum(v) == Math.signum(ov)) {
      return v;
    } else {
      return 0;
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
