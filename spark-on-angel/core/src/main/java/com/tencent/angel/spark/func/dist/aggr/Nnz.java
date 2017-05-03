package com.tencent.angel.spark.func.dist.aggr;

import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.aggr.PartitionAggrResult;

import java.nio.DoubleBuffer;
import java.util.List;

public final class Nnz extends UnaryAggrFunc {

  public Nnz(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public Nnz() {
    super();
  }

  @Override
  protected double doProcessRow(ServerDenseDoubleRow row) {
    int nnz = 0;
    DoubleBuffer data = row.getData();
    int size = row.size();
    for (int i = 0; i < size; i++) {
      if (data.get(i) != 0) nnz++;
    }
    return nnz;
  }

  @Override
  public AggrResult merge(List<PartitionAggrResult> partResults) {
    int nnz = 0;
    for (PartitionAggrResult partResult : partResults) {
      nnz += ((ScalarPartitionAggrResult) partResult).result;
    }

    return new ScalarAggrResult(nnz);
  }

}
