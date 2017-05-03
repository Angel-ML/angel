package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class V3FUpdaterFunc extends DefaultUpdaterFunc {

  public V3FUpdaterFunc(int matrixId, int rowId1, int rowId2, int rowId3, Serialize func) {
    super(new V3FUpdaterParam(matrixId, rowId1, rowId2, rowId3, func));
  }

  public V3FUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part = PSContext.get()
        .getMatrixPartitionManager()
        .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      V3FUpdaterParam.V3FPartitionUpdaterParam v3f =
          (V3FUpdaterParam.V3FPartitionUpdaterParam) partParam;
      ServerRow row1 = part.getRow(v3f.getRowId1());
      ServerRow row2 = part.getRow(v3f.getRowId2());
      ServerRow row3 = part.getRow(v3f.getRowId3());
      if (row1 != null && row2 != null && row3 != null) {
        update(row1, row2, row3, v3f.getFunc());
      }
    }
  }

  private void update(ServerRow row1, ServerRow row2, ServerRow row3, Serialize func) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate(
            (ServerDenseDoubleRow) row1,
            (ServerDenseDoubleRow) row2,
            (ServerDenseDoubleRow) row3,
            func);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(
      ServerDenseDoubleRow row1,
      ServerDenseDoubleRow row2,
      ServerDenseDoubleRow row3,
      Serialize func);

}
