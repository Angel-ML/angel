package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;
import java.util.Random;

public class RandomUniform extends VS2UpdaterFunc {

  public RandomUniform(int matrixId, int rowId, double min, double max) {
    super(matrixId, rowId, min, max);
  }

  public RandomUniform() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double min, double max) {
    Random rand = new Random(System.currentTimeMillis());
    double factor = max - min;

    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      data.put(i, factor * rand.nextDouble() + min);
    }
  }

}
