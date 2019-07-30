package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.List;

public class GetCloseness extends GetFunc {

  public GetCloseness(int matrixId, long[] nodes, long n) {
    super(new GetHyperLogLogParam(matrixId, nodes, n));
  }

  public GetCloseness(GetParam param) {
    super(param);
  }

  public GetCloseness() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetHyperLogLogPartParam param = (GetHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long n = param.getN();
    long[] nodes = param.getNodes();
    Long2DoubleOpenHashMap closenesses = new Long2DoubleOpenHashMap();
    for (int i = 0; i < nodes.length; i++) {
      HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) row.get(nodes[i]);
      if (hllElem.getCloseness() == 0 || hllElem.getCloseness() < n) {
        closenesses.put(nodes[i], 0);
      } else {
        closenesses.put(nodes[i], (double)n / (double)hllElem.getCloseness());
      }
    }
    return new GetClosenessPartResult(closenesses);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2DoubleOpenHashMap closenesses = new Long2DoubleOpenHashMap();
    for (PartitionGetResult r : partResults) {
      GetClosenessPartResult rr = (GetClosenessPartResult) r;
      closenesses.putAll(rr.getClosenesses());
    }
    return new GetClosenessResult(closenesses);
  }
}
