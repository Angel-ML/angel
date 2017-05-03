package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.ml.matrix.udf.aggr.DefaultAggrFunc;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrParam;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

public abstract class UnaryAggrFunc extends DefaultAggrFunc {

  public UnaryAggrFunc(int matrixId, int rowId) {
    super(new UnaryAggrParam(matrixId, rowId));
  }

  public UnaryAggrFunc() {
    super(null);
  }

  @Override
  public PartitionAggrResult aggr(PartitionAggrParam partKey) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partKey.getMatrixId(), partKey.getPartKey().getPartitionId());

    double result = 0.0;
    if (part != null) {
      int rowId = ((UnaryAggrParam.UnaryPartitionAggrParam) partKey).getRowId();
      ServerRow row = part.getRow(rowId);
      if (row != null) result = processRow(row);
    }

    return new ScalarPartitionAggrResult(result);
  }

  private double processRow(ServerRow row) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        return doProcessRow((ServerDenseDoubleRow) row);
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  protected abstract double doProcessRow(ServerDenseDoubleRow row);

}
