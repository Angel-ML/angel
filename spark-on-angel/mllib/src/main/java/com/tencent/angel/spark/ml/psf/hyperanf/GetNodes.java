package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexPartGetLongResult;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.spark.ml.psf.pagerank.GetNodesParam;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

public class GetNodes extends GetFunc {

  public GetNodes(int matrixId, int[] partitionIds) {
    this(new GetNodesParam(matrixId, partitionIds));
  }

  public GetNodes(GetParam param) {
    super(param);
  }

  public GetNodes() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partParam.getPartKey());
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);
    LongArrayList ret = new LongArrayList();
    for (long node = partParam.getPartKey().getStartCol(); node < partParam.getPartKey().getEndCol(); node++) {
      if (row.exist(node)) {
        ret.add(node);
      }
    }
    return new IndexPartGetLongResult(part.getPartitionKey(), ret.toLongArray());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    LongArrayList ret = new LongArrayList();
    for (PartitionGetResult result : partResults) {
      if (result instanceof IndexPartGetLongResult) {
        long[] values = ((IndexPartGetLongResult) result).getValues();
        for (int i = 0; i < values.length; i++)
          ret.add(values[i]);
      }
    }
    return new GetRowResult(ResponseType.SUCCESS,
        VFactory.denseLongVector(ret.toLongArray()));
  }
}
