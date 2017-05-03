package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.spark.func.Zip2MapFunc;

import java.nio.DoubleBuffer;

public class Zip2Map extends V3FUpdaterFunc {

  public Zip2Map(int matrixId, int fromId1, int fromId2, int toId, Zip2MapFunc func) {
    super(matrixId, fromId1, fromId2, toId, func);
  }

  public Zip2Map() {
    super();
  }

  @Override
  protected void doUpdate(
      ServerDenseDoubleRow fromRow1,
      ServerDenseDoubleRow fromRow2,
      ServerDenseDoubleRow toRow,
      Serialize func) {

    Zip2MapFunc mapper = (Zip2MapFunc) func;
    DoubleBuffer from1 = fromRow1.getData();
    DoubleBuffer from2 = fromRow2.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow1.size();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(from1.get(i), from2.get(i)));
    }
  }
}
