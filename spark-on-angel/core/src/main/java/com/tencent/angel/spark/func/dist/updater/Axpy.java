package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Axpy extends V2SUpdaterFunc {

  public Axpy(int matrixId, int xId, int yId, double a) {
    super(matrixId, xId, yId, a);
  }

  public Axpy() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow xRow, ServerDenseDoubleRow yRow, double a) {
    DoubleBuffer xData = xRow.getData();
    DoubleBuffer yData = yRow.getData();
    int size = xRow.size();
    for (int i = 0; i < size; i++) {
      yData.put(i, a * xData.get(i) + yData.get(i));
    }
  }

}
