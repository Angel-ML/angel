package com.tencent.angel.ml.matrix.psf.update.enhance.func;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import io.netty.buffer.ByteBuf;

public class Pow implements MapFunc {
  private double p;
  public Pow(double p) {
    this.p = p;
  }

  public Pow() {
    super();
  }

  @Override
  public double call(double value) {
    return Math.pow(value, p);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(p);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.p = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
