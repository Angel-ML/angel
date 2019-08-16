package com.tencent.angel.ml.matrix.psf.update.enhance.complex;

import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRows;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam;
import com.tencent.angel.ml.matrix.psf.update.update.PartIncrementRowsParam;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

import java.util.List;

public class Max extends IncrementRows {

  public Max(IncrementRowsParam param) {
    super(param);
  }

  public Max() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartIncrementRowsParam param = (PartIncrementRowsParam) partParam;
    List<RowUpdateSplit> updates = param.getUpdates();
    for (RowUpdateSplit update: updates) {
      ServerRow row = psContext.getMatrixStorageManager().getRow(param.getPartKey(), update.getRowId());
      row.startWrite();
      try {
        Vector vector = getVector(param.getMatrixId(), update.getRowId(), param.getPartKey());
        Ufuncs.imax(vector, update.getVector());
      } finally {
        row.endWrite();
      }
    }
  }

}
