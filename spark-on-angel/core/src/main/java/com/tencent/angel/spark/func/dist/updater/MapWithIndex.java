package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.spark.func.MapWithIndexFunc;

import java.nio.DoubleBuffer;

public class MapWithIndex extends V2FUpdaterFunc {

  public MapWithIndex(int matrixId, int fromId, int toId, MapWithIndexFunc func) {
    super(matrixId, fromId, toId, func);
  }

  public MapWithIndex() {
    super();
  }

  @Override
  protected void doUpdate(
      ServerDenseDoubleRow fromRow, ServerDenseDoubleRow toRow, Serialize func) {

    MapWithIndexFunc mapper = (MapWithIndexFunc) func;
    DoubleBuffer from = fromRow.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow.size();
    int startCol = fromRow.getStartCol();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(startCol + i, from.get(i)));
    }
  }
}
