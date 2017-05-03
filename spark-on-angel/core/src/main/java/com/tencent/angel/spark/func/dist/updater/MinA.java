package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class MinA extends VAUpdaterFunc {

  public MinA(int matrixId, int rowId, double[] other) {
    super(matrixId, rowId, other);
  }

  public MinA() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double[] other) {
    try {
      row.getLock().writeLock().lock();
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, Math.min(data.get(i), other[i]));
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

}
