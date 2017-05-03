package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;
import java.util.Random;

public class RandomNormal extends VS2UpdaterFunc {

  public RandomNormal(int matrixId, int rowId, double mean, double stddev) {
    super(matrixId, rowId, mean, stddev);
  }

  public RandomNormal() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double mean, double stdDev) {
    Random rand = new Random(System.currentTimeMillis());

    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      data.put(i, stdDev * rand.nextGaussian() + mean);
    }
  }

}
