package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.math2.vector.IntVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.func.IntElemUpdateFunc;

public abstract class ServerIntRow extends ServerRow {
  public ServerIntRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    IntVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
  }

  public abstract void elemUpdate(IntElemUpdateFunc func);
}
