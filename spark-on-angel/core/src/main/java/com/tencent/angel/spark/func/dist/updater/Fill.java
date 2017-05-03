package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Fill extends VSUpdaterFunc {

  public Fill(int matrixId, int rowId, double value) {
    super(matrixId, rowId, value);
  }

  public Fill() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double value) {
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      data.put(i, value);
    }
  }

}
