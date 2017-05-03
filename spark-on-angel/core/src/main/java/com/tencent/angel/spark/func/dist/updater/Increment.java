package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Increment extends VAUpdaterFunc {

  public Increment(int matrixId, int rowId, double[] delta) {
    super(matrixId, rowId, delta);
  }

  public Increment() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double[] delta) {
    try {
      row.getLock().writeLock().lock();
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, data.get(i) + delta[i]);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

}
