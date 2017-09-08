package com.tencent.angel.ml.matrix.psf.update.enhance;


import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.DoubleBuffer;

public class CompressUpdateFunc extends UpdateFunc {

  private static final Log LOG = LogFactory.getLog(CompressUpdateFunc.class);

  public CompressUpdateFunc(int matrixId, int rowId, double[] array, int bitPerItem) {
    super(new CompressUpdateParam(matrixId, rowId, array, bitPerItem));
  }

  public CompressUpdateFunc() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerPartition part =
        PSContext.get().getMatrixPartitionManager()
            .getPartition(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      CompressUpdateParam.CompressPartitionUpdateParam cp =
          (CompressUpdateParam.CompressPartitionUpdateParam) partParam;
      ServerRow row = part.getRow(cp.getRowId());
      if (row != null) {
        update(row, cp.getArraySlice());
      }
    }
  }

  private void update(ServerRow row, double[] arraySlice) {
    switch (row.getRowType()) {
      case T_DOUBLE_DENSE:
        doUpdate((ServerDenseDoubleRow) row, arraySlice);
        return;
      default:
        throw new RuntimeException("Spark on Angel currently only supports Double Dense Row");
    }
  }

  private void doUpdate(ServerDenseDoubleRow row, double[] arraySlice) {
    try {
      row.getLock().writeLock().lock();
      DoubleBuffer data = row.getData();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        data.put(i, data.get(i) + arraySlice[i]);
      }
    } finally {
      row.getLock().writeLock().unlock();
    }
  }

}
