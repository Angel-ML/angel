package com.tencent.angel.graph.client.node2vec.getfuncs.getprogress;

import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

import java.util.List;

public class GetProgress extends GetFunc {
  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public GetProgress(GetParam param) {
    super(param);
  }

  public GetProgress() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    int partitionId = partParam.getPartKey().getPartitionId();
    int finished = PathQueue.getProgress(partitionId);

    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);

    if (row.size() == finished) {
      return new GetProgressPartitionResult(true, 1.0);
    } else {
      return new GetProgressPartitionResult(false, 1.0 * finished / row.size());
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    GetProgressResult result = new GetProgressResult(true, 0.0);

    for (PartitionGetResult partResult : partResults) {
      GetProgressPartitionResult part = (GetProgressPartitionResult) partResult;
      result.setFinished(result.isFinished() && part.isFinished());
      result.setPrecent(result.getPrecent() + part.getPrecent());
    }

    result.setPrecent(result.getPrecent() / partResults.size());
    return result;
  }
}
