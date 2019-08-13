package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

import java.util.List;

public class NumNodes extends GetFunc {

  public NumNodes(int matrixId) {
    this(new GetParam(matrixId));
  }

  public NumNodes(GetParam param) {
    super(param);
  }

  public NumNodes() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);
    return new ScalarPartitionAggrResult(row.size());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    long numNodes = 0;
    for (PartitionGetResult result : partResults) {
      if (result instanceof ScalarPartitionAggrResult) {
        long value = (long)((ScalarPartitionAggrResult) result).result;
        numNodes += value;
      }
    }
    return new NumNodesResult(numNodes);
  }
}
