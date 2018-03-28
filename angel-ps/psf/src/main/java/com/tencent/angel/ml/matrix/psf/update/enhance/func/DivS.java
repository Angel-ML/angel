package com.tencent.angel.ml.matrix.psf.update.enhance.func;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import io.netty.buffer.ByteBuf;

public class DivS implements MapFunc {
  private double scalar;
  public DivS(double scalar) {
    assert(scalar != 0.0);
    this.scalar = scalar;
  }

  public DivS() {
    super();
  }

  @Override
  public double call(double value) {
    return value / scalar;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeDouble(scalar);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.scalar = buf.readDouble();
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
