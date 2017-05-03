package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Scale extends VSUpdaterFunc {

  public Scale(int matrixId, int rowId, double scaleFactor) {
    super(matrixId, rowId, scaleFactor);
  }

  public Scale() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, double scaleFactor) {
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      data.put(i, data.get(i) * scaleFactor);
    }
  }

}
