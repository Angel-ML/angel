package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Exp extends V2UpdaterFunc {

  public Exp(int matrixId, int fromId, int toId) {
    super(matrixId, fromId, toId);
  }

  public Exp() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow fromRow, ServerDenseDoubleRow toRow) {
    DoubleBuffer from = fromRow.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow.size();
    for (int i = 0; i < size; i++) {
      to.put(i, Math.exp(from.get(i)));
    }
  }

}
