package com.tencent.angel.graph.client.summary;

import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc;
import com.tencent.angel.ps.storage.vector.ServerRow;

public class NnzNode extends UnaryAggrFunc {

  public NnzNode(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public NnzNode() {
    super(-1, -1);
  }

  @Override
  public double mergeInit() {
    return 0;
  }

  @Override
  public double mergeOp(double a, double b) {
    return a + b;
  }

  @Override
  public double processRow(ServerRow row) {
    return row.size();
  }
}
