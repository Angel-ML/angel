package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;

public class Copy extends V2UpdaterFunc {

  public Copy(int matrixId, int sourceId, int targetId) {
    super(matrixId, sourceId, targetId);
  }

  public Copy() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow sourceRow, ServerDenseDoubleRow targetRow) {
    DoubleBuffer source = sourceRow.getData();
    DoubleBuffer target = targetRow.getData();
    int size = sourceRow.size();
    for (int i = 0; i < size; i++) {
      target.put(i, source.get(i));
    }
  }

}
