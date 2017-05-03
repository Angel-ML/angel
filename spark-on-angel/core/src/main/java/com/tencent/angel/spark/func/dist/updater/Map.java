package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.spark.func.MapFunc;

import java.nio.DoubleBuffer;

public class Map extends V2FUpdaterFunc {

  public Map(int matrixId, int fromId, int toId, MapFunc func) {
    super(matrixId, fromId, toId, func);
  }

  public Map() {
    super();
  }

  @Override
  protected void doUpdate(
    ServerDenseDoubleRow fromRow,
    ServerDenseDoubleRow toRow,
    Serialize func) {
    MapFunc mapper = (MapFunc) func;
    DoubleBuffer from = fromRow.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow.size();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(from.get(i)));
    }
  }
}
