package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class DivS extends V2SUpdaterFunc {

  public DivS(int matrixId, int fromId, int toId, double value) {
    super(matrixId, fromId, toId, value);
  }

  public DivS() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow fromRow, ServerDenseDoubleRow toRow, double value) {
    DoubleBuffer from = fromRow.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow.size();
    for (int i = 0; i < size; i++) {
      to.put(i, from.get(i) / value);
    }
  }

}
