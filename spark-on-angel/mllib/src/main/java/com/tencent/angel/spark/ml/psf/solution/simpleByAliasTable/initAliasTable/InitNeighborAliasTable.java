package com.tencent.angel.spark.ml.psf.solution.simpleByAliasTable.initAliasTable;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerIntAnyRow;
import com.tencent.angel.spark.ml.psf.solution.simpleByAliasTable.sampleAliasTable.NeighborsAliasTableElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitNeighborAliasTable extends UpdateFunc {

  private static final Logger LOG = LoggerFactory.getLogger(InitNeighborAliasTable.class);

  /**
   * Create a new UpdateParam
   */
  public InitNeighborAliasTable(InitNeighborAliasTableParam param) {
    super(param);
  }

  public InitNeighborAliasTable() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNeighborAliasTableParam param = (PartInitNeighborAliasTableParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
        .getPartition(partParam.getPartKey().getPartitionId());
    ServerIntAnyRow row = (ServerIntAnyRow) part.getRow(0);

    ObjectIterator<Int2ObjectMap.Entry<NeighborsAliasTableElement>> iter = param
        .getNodeId2Neighbors().int2ObjectEntrySet().iterator();

    row.startWrite();
    try {
      while (iter.hasNext()) {
        Int2ObjectMap.Entry<NeighborsAliasTableElement> entry = iter.next();
        NeighborsAliasTableElement element = entry.getValue();
        if (element == null) {
          row.set(entry.getIntKey(), null);
        } else {
          row.set(entry.getIntKey(),
              new NeighborsAliasTableElement(element.getNeighborIds(), element.getAccept(),
                  element.getAlias()));
        }
      }
    } finally {
      row.endWrite();
    }
  }
}
