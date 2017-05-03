package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class VAUpdaterFunc extends DefaultUpdaterFunc {

  public VAUpdaterFunc(int matrixId, int rowId, double[] array) {
    super(new VAUpdaterParam(matrixId, rowId, array));
  }

  public VAUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      VAUpdaterParam.VAPartitionUpdaterParam va =
          (VAUpdaterParam.VAPartitionUpdaterParam) partParam;
      ServerRow row = part.getRow(va.getRowId());
      if (row != null) {
        update(row, va.getArraySlice());
      }
    }
  }

  private void update(ServerRow row, double[] arraySlice) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row, arraySlice);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow row, double[] arraySlice);

}
