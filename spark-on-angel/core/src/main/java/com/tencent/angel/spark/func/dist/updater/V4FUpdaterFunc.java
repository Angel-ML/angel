package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class V4FUpdaterFunc extends DefaultUpdaterFunc {

  public V4FUpdaterFunc(
      int matrixId, int rowId1, int rowId2, int rowId3, int rowId4, Serialize func) {
    super(new V4FUpdaterParam(matrixId, rowId1, rowId2, rowId3, rowId4, func));
  }

  public V4FUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part = PSContext.get()
        .getMatrixPartitionManager()
        .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      V4FUpdaterParam.V4FPartitionUpdaterParam v4f =
          (V4FUpdaterParam.V4FPartitionUpdaterParam) partParam;
      ServerRow row1 = part.getRow(v4f.getRowId1());
      ServerRow row2 = part.getRow(v4f.getRowId2());
      ServerRow row3 = part.getRow(v4f.getRowId3());
      ServerRow row4 = part.getRow(v4f.getRowId4());
      if (row1 != null && row2 != null && row3 != null && row4 != null) {
        update(row1, row2, row3, row4, v4f.getFunc());
      }
    }
  }

  private void update(
      ServerRow row1, ServerRow row2, ServerRow row3, ServerRow row4, Serialize func) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate(
            (ServerDenseDoubleRow) row1, (ServerDenseDoubleRow) row2,
            (ServerDenseDoubleRow) row3, (ServerDenseDoubleRow) row4,
            func);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(
      ServerDenseDoubleRow row1, ServerDenseDoubleRow row2,
      ServerDenseDoubleRow row3, ServerDenseDoubleRow row4,
      Serialize func);

}
