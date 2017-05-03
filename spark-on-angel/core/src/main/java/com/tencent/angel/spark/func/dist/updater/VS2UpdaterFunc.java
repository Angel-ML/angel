package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class VS2UpdaterFunc extends DefaultUpdaterFunc {

  public VS2UpdaterFunc(int matrixId, int rowId, double scalar1, double scalar2) {
    super(new VS2UpdaterParam(matrixId, rowId, scalar1, scalar2));
  }

  public VS2UpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      VS2UpdaterParam.VS2PartitionUpdaterParam vs2 =
          (VS2UpdaterParam.VS2PartitionUpdaterParam) partParam;
      ServerRow row = part.getRow(vs2.getRowId());
      if (row != null) {
        update(row, vs2.getScalar1(), vs2.getScalar2());
      }
    }
  }

  private void update(ServerRow row, double scalar1, double scalar2) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row, scalar1, scalar2);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow row, double scalar1, double scalar2);

}
