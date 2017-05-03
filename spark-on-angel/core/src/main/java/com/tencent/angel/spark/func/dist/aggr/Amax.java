package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

import java.nio.DoubleBuffer;
import java.util.List;

public final class Amax extends UnaryAggrFunc {

  public Amax(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Amax() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double amax = Double.MIN_VALUE;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      amax = Math.max(amax, Math.abs(data.get(i)));
    }
    return amax;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double max = Double.MIN_VALUE;
    for (PartitionAggrResult partResult : partResults) {
      max = Math.max(max, ((ScalarPartitionAggrResult) partResult).result);
    }

    return new ScalarAggrResult(max);
  }

}
