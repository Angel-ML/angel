package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

import java.nio.DoubleBuffer;
import java.util.List;

public final class Max extends UnaryAggrFunc {

  public Max(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Max() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double max = Double.NEGATIVE_INFINITY;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      max = Math.max(max, data.get(i));
    }
    return max;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double max = Double.NEGATIVE_INFINITY;
    for (PartitionAggrResult partResult : partResults) {
      max = Math.max(max, ((ScalarPartitionAggrResult) partResult).result);
    }

    return new ScalarAggrResult(max);
  }

}
