package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.math2.vector.DoubleVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.func.DoubleElemUpdateFunc;

public abstract class ServerDoubleRow extends ServerRow {
  public ServerDoubleRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    DoubleVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
  }

  public abstract void elemUpdate(DoubleElemUpdateFunc func);
}
