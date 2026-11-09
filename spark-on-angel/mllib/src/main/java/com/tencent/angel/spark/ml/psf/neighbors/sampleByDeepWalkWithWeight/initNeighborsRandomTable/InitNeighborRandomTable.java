package com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.initNeighborsRandomTable;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.sampleRandomNeighborTable.NeighborsRandomTableElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitNeighborRandomTable extends UpdateFunc {

  private static final Logger LOG = LoggerFactory.getLogger(InitNeighborRandomTable.class);

  /**
   * Create a new UpdateParam
   */
  public InitNeighborRandomTable(InitNeighborRandomTableParam param) {
    super(param);
  }

  public InitNeighborRandomTable() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNeighborRandomTableParam param = (PartInitNeighborRandomTableParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
        .getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

    ObjectIterator<Long2ObjectMap.Entry<NeighborsRandomTableElement>> iter = param
        .getNodeId2Neighbors().long2ObjectEntrySet().iterator();
    row.startWrite();
    try {
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<NeighborsRandomTableElement> entry = iter.next();
        NeighborsRandomTableElement element = entry.getValue();
        if (element == null) {
          row.set(entry.getLongKey(), null);
        } else {
          row.set(entry.getLongKey(),
              new NeighborsRandomTableElement(element.getNeighborIds(), element.getWeights()));
        }
      }
    } finally {
      row.endWrite();
    }
  }
}
