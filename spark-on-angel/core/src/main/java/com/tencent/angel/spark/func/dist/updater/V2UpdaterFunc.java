package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class V2UpdaterFunc extends DefaultUpdaterFunc {

  public V2UpdaterFunc(int matrixId, int rowId1, int rowId2) {
    super(new V2UpdaterParam(matrixId, rowId1, rowId2));
  }

  public V2UpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      V2UpdaterParam.V2PartitionUpdaterParam v2 =
          (V2UpdaterParam.V2PartitionUpdaterParam) partParam;
      ServerRow row1 = part.getRow(v2.getRowId1());
      ServerRow row2 = part.getRow(v2.getRowId2());
      if (row1 != null && row2 != null) {
        update(row1, row2);
      }
    }
  }

  private void update(ServerRow row1, ServerRow row2) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row1, (ServerDenseDoubleRow) row2);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow row1, ServerDenseDoubleRow row2);

}
