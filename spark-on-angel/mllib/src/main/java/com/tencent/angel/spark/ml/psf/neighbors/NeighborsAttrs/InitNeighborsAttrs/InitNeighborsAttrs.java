package com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitNeighborsAttrs extends UpdateFunc {

  private static final Logger LOG = LoggerFactory.getLogger(InitNeighborsAttrs.class);

  /**
   * Create a new UpdateParam
   */
  public InitNeighborsAttrs(InitNeighborsAttrsParam param) {
    super(param);
  }

  public InitNeighborsAttrs() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNeighborsAttrsParam param = (PartInitNeighborsAttrsParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
        .getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

    ObjectIterator<Long2ObjectMap.Entry<NeighborsAttrsElement>> iter = param.getNodeId2Neighbors()
        .long2ObjectEntrySet().iterator();

    row.startWrite();
    try {
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<NeighborsAttrsElement> entry = iter.next();
        NeighborsAttrsElement element = entry.getValue();
        if (element == null) {
          row.set(entry.getLongKey(), null);
        } else {
          row.set(entry.getLongKey(),
              new NeighborsAttrsElement(element.getNeighborIds(), element.getAttrs()));
        }
      }
    } finally {
      row.endWrite();
    }
  }
}
