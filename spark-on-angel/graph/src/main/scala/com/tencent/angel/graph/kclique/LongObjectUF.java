package com.tencent.angel.graph.kclique;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Init node neighbors for long type node id
 */
public class LongObjectUF extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public LongObjectUF(KCliqueUpdateParam param) {
    super(param);
  }

  public LongObjectUF() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    KCliquePartUpdateParam param = (KCliquePartUpdateParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
            .getPartition(partParam.getPartKey().getPartitionId());

    int writeRowId = param.getWriteRowId();
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(writeRowId);

    ObjectIterator<Long2ObjectMap.Entry<long[]>> iter = param
            .getNodeIdToNeighborIndices().long2ObjectEntrySet().iterator();

    row.startWrite();
    try {
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<long[]> entry = iter.next();
        row.set(entry.getLongKey(), new LongArrayElement(entry.getValue()));
      }
    } finally {
      row.endWrite();
    }
  }
}
