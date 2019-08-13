package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitHyperLogLog extends UpdateFunc {

  public InitHyperLogLog(int matrixId, int p, int sp, long[] nodes) {
    super(new InitHyperLogLogParam(matrixId, p, sp, nodes));
  }

  public InitHyperLogLog(InitHyperLogLogParam param) {
    super(param);
  }

  public InitHyperLogLog() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParm) {
    InitHyperLogLogPartParam param = (InitHyperLogLogPartParam) partParm;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    int p = param.getP();
    int sp = param.getSp();
    long[] nodes = param.getNodes();
    row.startWrite();
    try {
      for (int i = 0; i < nodes.length; i++)
        row.set(nodes[i], new HyperLogLogPlusElement(nodes[i], p, sp));
    } finally {
      row.endWrite();
    }

  }

}
