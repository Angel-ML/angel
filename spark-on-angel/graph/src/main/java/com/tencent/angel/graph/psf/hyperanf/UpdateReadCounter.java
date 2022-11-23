package com.tencent.angel.graph.psf.hyperanf;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
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
public class UpdateReadCounter extends UpdateFunc {

  public UpdateReadCounter(UpdateReadCounterParam param) {
    super(param);
  }

  public UpdateReadCounter(int matrixId) {
    super(new UpdateReadCounterParam(matrixId));
  }

  public UpdateReadCounter() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    //UpdateReadCounterPartParam param = (UpdateReadCounterPartParam) partParam;
    //ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);
    UpdateReadCounterPartParam param = (UpdateReadCounterPartParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      HyperLogLogElement hllElem = (HyperLogLogElement) iter.next().getValue();
      if (hllElem.isActive()) {
        hllElem.updateReadCounter();
      }
    }
  }
}