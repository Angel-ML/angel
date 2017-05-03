package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class VSUpdaterFunc extends DefaultUpdaterFunc {

  public VSUpdaterFunc(int matrixId, int rowId, double scalar) {
    super(new VSUpdaterParam(matrixId, rowId, scalar));
  }

  public VSUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      VSUpdaterParam.VSPartitionUpdaterParam vs =
          (VSUpdaterParam.VSPartitionUpdaterParam) partParam;
      ServerRow row = part.getRow(vs.getRowId());
      if (row != null) {
        update(row, vs.getScalar());
      }
    }
  }

  private void update(ServerRow row, double scalar) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row, scalar);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow row, double scalar);

}
