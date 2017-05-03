package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.spark.func.Zip3MapWithIndexFunc;

import java.nio.DoubleBuffer;

public class Zip3MapWithIndex extends V4FUpdaterFunc {

  public Zip3MapWithIndex(
      int matrixId, int fromId1, int fromId2, int fromId3, int toId, Zip3MapWithIndexFunc func) {
    super(matrixId, fromId1, fromId2, fromId3, toId, func);
  }

  public Zip3MapWithIndex() {
    super();
  }

  @Override
  protected void doUpdate(
      ServerDenseDoubleRow fromRow1, ServerDenseDoubleRow fromRow2, ServerDenseDoubleRow fromRow3,
      ServerDenseDoubleRow toRow,
      Serialize func) {
    Zip3MapWithIndexFunc mapper = (Zip3MapWithIndexFunc) func;
    DoubleBuffer from1 = fromRow1.getData();
    DoubleBuffer from2 = fromRow2.getData();
    DoubleBuffer from3 = fromRow3.getData();
    DoubleBuffer to = toRow.getData();
    int startCol = fromRow1.getStartCol();
    int size = fromRow1.size();
    for (int i = 0; i < size; i++) {
      to.put(i, mapper.call(startCol + i, from1.get(i), from2.get(i), from3.get(i)));
    }
  }
}
