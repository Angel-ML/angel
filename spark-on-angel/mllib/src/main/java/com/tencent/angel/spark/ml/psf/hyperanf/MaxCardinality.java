package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class MaxCardinality extends UnaryAggrFunc {

  public MaxCardinality(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public MaxCardinality() {
    super(-1, -1);
  }

  @Override
  public double processRow(ServerRow row) {
    double maxCardinality = 0;
    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = ((ServerLongAnyRow) row).iterator();
    while (it.hasNext()) {
      maxCardinality = Math.max(maxCardinality, ((HyperLogLogPlusElement)it.next().getValue()).getCardinality());
    }
    return maxCardinality;
  }

  @Override
  public double mergeOp(double a, double b) {
    return Math.max(a, b);
  }

  @Override
  public double mergeInit() {
    return 0;
  }
}
