package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple2;
import scala.Tuple3;

import java.util.List;

public class GetClosenessAndCardinality extends GetFunc {

  public GetClosenessAndCardinality(int matrixId, long[] nodes, long n) {
    super(new GetHyperLogLogParam(matrixId, nodes, n));
  }

  public GetClosenessAndCardinality(GetParam param) {
    super(param);
  }

  public GetClosenessAndCardinality() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetHyperLogLogPartParam param = (GetHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long n = param.getN();
    long[] nodes = param.getNodes();
    Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses = new Long2ObjectOpenHashMap<>();
    for (int i = 0; i < nodes.length; i++) {
      HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) row.get(nodes[i]);
      if (hllElem.getCloseness() < n) {
        closenesses.put(nodes[i], new Tuple3<>(0d, hllElem.getCardinality(), hllElem.getCloseness()));
      } else {
        closenesses.put(nodes[i], new Tuple3<>(((double)n / (double)hllElem.getCloseness()),
            hllElem.getCardinality(),
            hllElem.getCloseness()));
      }
    }
    return new GetClosenessAndCardinalityPartResult(closenesses);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses = new Long2ObjectOpenHashMap<>();
    for (PartitionGetResult r : partResults) {
      GetClosenessAndCardinalityPartResult rr = (GetClosenessAndCardinalityPartResult) r;
      closenesses.putAll(rr.getClosenesses());
    }
    return new GetClosenessAndCardinalityResult(closenesses);
  }
}
