package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

import java.nio.DoubleBuffer;
import java.util.List;

public class Dot extends BinaryAggrFunc {

  public Dot(int matrixId, int rowId1, int rowId2) {
    super(matrixId, rowId1, rowId2);
  }

  public Dot() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row1, ServerDenseDoubleRow row2) {
    double sum = 0.0;
    DoubleBuffer data1 = row1.getData();
    DoubleBuffer data2 = row2.getData();
    int size = row1.size();
    for (int i = 0; i < size; i++) {
      sum += data1.get(i) * data2.get(i);
    }
    return sum;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    double sum = 0.0;
    for (PartitionAggrResult partResult : partResults) {
      sum += ((ScalarPartitionAggrResult) partResult).result;
    }

    return new ScalarAggrResult(sum);
  }

}
