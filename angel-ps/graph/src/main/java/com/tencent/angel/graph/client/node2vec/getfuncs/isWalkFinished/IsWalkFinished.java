package com.tencent.angel.graph.client.node2vec.getfuncs.isWalkFinished;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.List;

public class IsWalkFinished extends GetFunc {

  public IsWalkFinished(GetParam param) {
    super(param);
  }

  public IsWalkFinished() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    IsWalkFinishedPartitionParam pparam = (IsWalkFinishedPartitionParam) partParam;
    PartitionKey partKey = pparam.getPartKey();
    ServerLongAnyRow walkPath = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(partKey, 0);

    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = walkPath.iterator();
    walkPath.startWrite();
    PathQueue.clear(partKey.getPartitionId());
    int count = 0;
    int batchSize = 1024;
    try {
      ArrayList<WalkPath> batchPath = new ArrayList<>();
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<IElement> entry = iter.next();
        WalkPath wPath = (WalkPath) entry.getValue();

        if (!wPath.isComplete()) {
          batchPath.add(wPath);
          count++;
          if (count % batchSize == 0) {
            PathQueue.initPushBatch(partKey.getPartitionId(), batchPath);
            batchPath.clear();
          }
        }
      }

      if (!batchPath.isEmpty()) {
        PathQueue.initPushBatch(partKey.getPartitionId(), batchPath);
        batchPath.clear();
      }
    } finally {
      walkPath.endWrite();
    }

    return new IsWalkFinishedPartitionResult(count);
  }


  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    long count = 0;

    for (PartitionGetResult partResult : partResults) {
      count += ((IsWalkFinishedPartitionResult) partResult).getCount();
    }

    return new IsWalkFinishedResult(count);
  }
}
