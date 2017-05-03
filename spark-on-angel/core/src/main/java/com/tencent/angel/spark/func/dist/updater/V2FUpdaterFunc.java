package com.tencent.angel.spark.func.dist.updater;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.updater.DefaultUpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.PartitionUpdaterParam;

public abstract class V2FUpdaterFunc extends DefaultUpdaterFunc {

  public V2FUpdaterFunc(int matrixId, int rowId1, int rowId2, Serialize func) {
    super(new V2FUpdaterParam(matrixId, rowId1, rowId2, func));
  }

  public V2FUpdaterFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      V2FUpdaterParam.V2FPartitionUpdaterParam v2f =
          (V2FUpdaterParam.V2FPartitionUpdaterParam) partParam;
      ServerRow row1 = part.getRow(v2f.getRowId1());
      ServerRow row2 = part.getRow(v2f.getRowId2());
      if (row1 != null && row2 != null) {
        update(row1, row2, v2f.getFunc());
      }
    }
  }

  private void update(ServerRow row1, ServerRow row2, Serialize func) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row1, (ServerDenseDoubleRow) row2, func);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract void doUpdate(ServerDenseDoubleRow row1,
                                   ServerDenseDoubleRow row2,
                                   Serialize func);

}
