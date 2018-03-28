package com.tencent.angel.ml.matrix.psf.update.enhance.func;

import com.tencent.angel.ml.matrix.psf.update.enhance.map.MapFunc;
import io.netty.buffer.ByteBuf;

public class Log1p implements MapFunc {
  public Log1p() {
    super();
  }

  @Override
  public double call(double value) {
    return Math.log1p(value);
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
