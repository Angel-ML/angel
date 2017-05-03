package com.tencent.angel.spark.ml.pof;

import com.tencent.angel.spark.func.MapFunc;
import io.netty.buffer.ByteBuf;

public class ADMMZUpdater implements MapFunc {
  private double kappa;
  public ADMMZUpdater(double kappa) {
    this.kappa = kappa;
  }

  public ADMMZUpdater() {
    super();
  }

  @Override
  public double call(double x) {
    return Math.max(0, x - kappa) - Math.max(0, -x - kappa);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(kappa);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    kappa = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }

}
