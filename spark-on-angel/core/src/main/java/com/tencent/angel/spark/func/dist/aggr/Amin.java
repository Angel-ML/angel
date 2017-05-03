package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;

import java.nio.DoubleBuffer;
import java.util.List;

public final class Amin extends UnaryAggrFunc {

  public Amin(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Amin() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double amin = Double.MAX_VALUE;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      amin = Math.min(amin, Math.abs(data.get(i)));
    }
    return amin;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double min = Double.MAX_VALUE;
    for (PartitionAggrResult partResult : partResults) {
      min = Math.min(min, ((ScalarPartitionAggrResult) partResult).result);
    }

    return new ScalarAggrResult(min);
  }

}
