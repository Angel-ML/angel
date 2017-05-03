package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.spark.func.Zip2MapWithIndexFunc;

import java.nio.DoubleBuffer;

public class Zip2MapWithIndex extends V3FUpdaterFunc {

  public Zip2MapWithIndex(
      int matrixId, int fromId1, int fromId2, int toId, Zip2MapWithIndexFunc func) {
    super(matrixId, fromId1, fromId2, toId, func);
  }

  public Zip2MapWithIndex() {
    super();
  }

  @Override
  protected void doUpdate(
      ServerDenseDoubleRow fromRow1,
      ServerDenseDoubleRow fromRow2,
      ServerDenseDoubleRow toRow, Serialize func) {

    Zip2MapWithIndexFunc mapper = (Zip2MapWithIndexFunc) func;
    DoubleBuffer from1 = fromRow1.getData();
    DoubleBuffer from2 = fromRow2.getData();
    DoubleBuffer to = toRow.getData();
    int size = fromRow1.size();
    int startCol = fromRow1.getStartCol();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(startCol + i, from1.get(i), from2.get(i)));
    }
  }
}
