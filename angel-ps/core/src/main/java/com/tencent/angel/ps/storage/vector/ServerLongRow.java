package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.math2.vector.LongVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.func.LongElemUpdateFunc;

public abstract class ServerLongRow extends ServerRow {
  public ServerLongRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    LongVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
  }

  public abstract void elemUpdate(LongElemUpdateFunc func);
}
