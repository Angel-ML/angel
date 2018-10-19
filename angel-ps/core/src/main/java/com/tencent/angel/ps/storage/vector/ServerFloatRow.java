package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.func.FloatElemUpdateFunc;

public abstract class ServerFloatRow extends ServerRow {
  public ServerFloatRow(int rowId, RowType rowType, long startCol, long endCol, int estElemNum,
    FloatVector innerRow) {
    super(rowId, rowType, startCol, endCol, estElemNum, innerRow);
  }

  public abstract void elemUpdate(FloatElemUpdateFunc func);
}
