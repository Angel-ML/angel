package com.tencent.angel.ml.matrix.psf.update.enhance.func;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import io.netty.buffer.ByteBuf;

public class Scale implements MapFunc {
  private double factor;
  public Scale(double factor) {
    this.factor = factor;
  }

  public Scale() {
    super();
  }

  @Override
  public double call(double value) {
    return value * factor;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(factor);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.factor = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
