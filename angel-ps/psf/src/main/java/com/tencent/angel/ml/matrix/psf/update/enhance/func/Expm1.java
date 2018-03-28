package com.tencent.angel.ml.matrix.psf.update.enhance.func;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import io.netty.buffer.ByteBuf;

public class Expm1 implements MapFunc {
  public Expm1() {
    super();
  }

  @Override
  public double call(double value) {
    return Math.expm1(value);
  }

  @Override
  public void serialize(ByteBuf buf) {
  }

  @Override
  public void deserialize(ByteBuf buf) {
  }

  @Override
  public int bufferLen() {
    return 0;
  }
}
