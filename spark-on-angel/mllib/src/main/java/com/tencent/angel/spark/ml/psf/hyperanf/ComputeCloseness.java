package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;


/**
 * UpdateFunc for computing the cardinality and closeness without normalization,
 * updating the activeness status, and switching the read/write counter
 */
public class ComputeCloseness extends UpdateFunc {

  public ComputeCloseness(ComputeClosenessParam param) {
    super(param);
  }

  public ComputeCloseness(int matrixId, int r) {
    super(new ComputeClosenessParam(matrixId, r));
  }

  public ComputeCloseness() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ComputeClosenessPartParam param = (ComputeClosenessPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);
    int r = param.getR();

    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) iter.next().getValue();
      if (hllElem.isActive()) {
        hllElem.updateCloseness(r);
      }
    }
  }
}
