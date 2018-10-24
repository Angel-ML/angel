package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.common.Serialize;
import io.netty.buffer.ByteBuf;

import java.util.Random;

public class RandomNormalInitFunc implements InitFunc {
  private double mean;
  private double stdDev;
  private Random rand = new Random(System.currentTimeMillis());

  public RandomNormalInitFunc(double mean, double stdDev) {
    this.mean = mean;
    this.stdDev = stdDev;
  }

  public RandomNormalInitFunc() {
    this(0.0, 0.0);
  }

  @Override
  public double action() {
    return stdDev * rand.nextGaussian() + mean;
  }

  @Override public void serialize(ByteBuf buf) {
    buf.writeDouble(mean);
    buf.writeDouble(stdDev);
  }

  @Override public void deserialize(ByteBuf buf) {
    this.mean = buf.readDouble();
    this.stdDev = buf.readDouble();
  }

  @Override public int bufferLen() {
    return 16;
  }
}
