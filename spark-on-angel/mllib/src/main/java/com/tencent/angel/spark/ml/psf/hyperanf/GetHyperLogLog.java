package com.tencent.angel.spark.ml.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetHyperLogLog extends GetFunc {

  public GetHyperLogLog(GetHyperLogLogParam param) {
    super(param);
  }

  public GetHyperLogLog(int matrixId, long[] nodes) {
    super(new GetHyperLogLogParam(matrixId, nodes, 0L));
  }

  public GetHyperLogLog() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetHyperLogLogPartParam param = (GetHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long[] nodes = param.getNodes();
    Long2ObjectOpenHashMap<HyperLogLogPlus> logs = new Long2ObjectOpenHashMap<>();
    row.startRead(20000);
    try {
      for (int i = 0; i < nodes.length; i++) {
        HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) row.get(nodes[i]);
        if (hllElem.isActive()) {
          logs.put(nodes[i], hllElem.getHyperLogLogPlus());
        }
      }
    } finally {
      row.endRead();
    }
    return new GetHyperLogLogPartResult(logs);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<HyperLogLogPlus> logs = new Long2ObjectOpenHashMap<>();
    for (PartitionGetResult r: partResults) {
      GetHyperLogLogPartResult rr = (GetHyperLogLogPartResult) r;
      logs.putAll(rr.getLogs());
    }

    return new GetHyperLogLogResult(logs);
  }
}
