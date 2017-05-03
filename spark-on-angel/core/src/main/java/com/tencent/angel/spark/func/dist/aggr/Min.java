package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

import java.nio.DoubleBuffer;
import java.util.List;

public class Min extends UnaryAggrFunc {

  public Min(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Min() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double min = Double.POSITIVE_INFINITY;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      min = Math.min(min, data.get(i));
    }
    return min;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double min = Double.POSITIVE_INFINITY;
    for (PartitionAggrResult partResult : partResults) {
      min = Math.min(min, ((ScalarPartitionAggrResult) partResult).result);
    }

    return new ScalarAggrResult(min);
  }

}
