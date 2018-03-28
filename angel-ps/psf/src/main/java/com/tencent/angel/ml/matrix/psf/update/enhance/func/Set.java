package com.tencent.angel.ml.matrix.psf.update.enhance.func;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import io.netty.buffer.ByteBuf;

public class Set implements MapFunc {
  private double p;
  public Set(double p) {
    this.p = p;
  }

  public Set() {
    super();
  }

  @Override
  public double call(double value) {
    return p;
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
