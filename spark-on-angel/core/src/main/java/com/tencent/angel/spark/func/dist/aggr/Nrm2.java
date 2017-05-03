package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

import java.nio.DoubleBuffer;
import java.util.List;

public final class Nrm2 extends UnaryAggrFunc {

  public Nrm2(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Nrm2() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    double qSum = 0;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      qSum += Math.pow(data.get(i), 2);
    }
    return qSum;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double sum = 0;
    for (PartitionAggrResult partResult : partResults) {
      sum += ((ScalarPartitionAggrResult) partResult).result;
    }

    return new ScalarAggrResult(Math.sqrt(sum));
  }

}
