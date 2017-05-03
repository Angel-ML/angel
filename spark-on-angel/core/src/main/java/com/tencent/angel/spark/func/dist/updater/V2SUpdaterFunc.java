package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class V2SUpdaterFunc extends DefaultUpdaterFunc {

  public V2SUpdaterFunc(int matrixId, int rowId1, int rowId2, double scalar) {
    super(new V2SUpdaterParam(matrixId, rowId1, rowId2, scalar));
  }

  public V2SUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part = PSContext.get()
      .getMatrixPartitionManager()
      .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      V2SUpdaterParam.V2SPartitionUpdaterParam v2s =
          (V2SUpdaterParam.V2SPartitionUpdaterParam) partParam;
      ServerRow row1 = part.getRow(v2s.getRowId1());
      ServerRow row2 = part.getRow(v2s.getRowId2());
      if (row1 != null && row2 != null) {
        update(row1, row2, v2s.getScalar());
      }
    }
  }

  private void update(ServerRow row1, ServerRow row2, double scalar) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row1, (ServerDenseDoubleRow) row2, scalar);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(
      ServerDenseDoubleRow row1, ServerDenseDoubleRow row2, double scalar);

}
