package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Sqrt extends V2UpdaterFunc {

  public Sqrt(int matrixId, int fromId, int toId) {
    super(matrixId, fromId, toId);
  }

  public Sqrt() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow fromRow, ServerDenseDoubleRow toRow) {
    DoubleBuffer from = fromRow.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow.size();
    for (int i = 0; i < size; i++) {
      to.put(i, Math.sqrt(from.get(i)));
    }
  }

}
