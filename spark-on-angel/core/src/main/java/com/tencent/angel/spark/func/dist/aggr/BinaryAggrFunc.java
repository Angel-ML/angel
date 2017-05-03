package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.aggr.DefaultAggrFunc;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrParam;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

public abstract class BinaryAggrFunc extends DefaultAggrFunc {

  public BinaryAggrFunc(int matrixId, int rowId1, int rowId2) {
    super(new BinaryAggrParam(matrixId, rowId1, rowId2));
  }

  public BinaryAggrFunc() {
    super(null);
  }

  @Override
  public PartitionAggrResult aggr(PartitionAggrParam partKey) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());
    int rowId1 = ((BinaryAggrParam.BinaryPartitionAggrParam) partKey).getRowId1();
    int rowId2 = ((BinaryAggrParam.BinaryPartitionAggrParam) partKey).getRowId2();

    double result = 0.0;
    if (part != null) {
      ServerRow row1 = part.getRow(rowId1);
      ServerRow row2 = part.getRow(rowId2);
      if (row1 != null && row2 != null) result = processRows(row1, row2);
    }

    return new ScalarPartitionAggrResult(result);
  }

  private double processRows(ServerRow row1, ServerRow row2) {
    switch (row1.getRowType()) {
      case T_DOUBLE_DENSE:
        return doProcessRow((ServerDenseDoubleRow) row1, (ServerDenseDoubleRow) row2);
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract double doProcessRow(ServerDenseDoubleRow row1, ServerDenseDoubleRow row2);

}
