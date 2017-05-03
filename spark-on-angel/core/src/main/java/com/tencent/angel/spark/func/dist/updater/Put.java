package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Put extends VAUpdaterFunc {

  public Put(int matrixId, int rowId, double[] values) {
    super(matrixId, rowId, values);
  }

  public Put() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double[] values) {
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      data.put(i, values[i]);
    }
  }

}
