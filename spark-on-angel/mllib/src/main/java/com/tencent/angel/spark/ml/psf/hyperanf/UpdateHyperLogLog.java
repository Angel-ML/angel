package com.tencent.angel.spark.ml.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class UpdateHyperLogLog extends UpdateFunc {

  public UpdateHyperLogLog(UpdateHyperLogLogParam param) {
    super(param);
  }

  public UpdateHyperLogLog(int matrixId, Long2ObjectOpenHashMap<HyperLogLogPlus> updates, int p, int sp) {
    super(new UpdateHyperLogLogParam(matrixId, updates, p, sp));
  }

  public UpdateHyperLogLog() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    UpdateHyperLogLogPartParam param = (UpdateHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);
    Long2ObjectOpenHashMap<HyperLogLogPlus> updates = param.getUpdates();
    ObjectIterator<Long2ObjectMap.Entry<HyperLogLogPlus>> it =
        updates.long2ObjectEntrySet().fastIterator();
    row.startWrite();
    try {
      while (it.hasNext()) {
        Long2ObjectMap.Entry<HyperLogLogPlus> entry = it.next();
        long outNode = entry.getLongKey();
        HyperLogLogPlus outCounter = entry.getValue();
        if (!row.exist(outNode)) {
          row.set(outNode,new HyperLogLogPlusElement(outNode, param.getP(), param.getSp()));
        }
        HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) row.get(outNode);
        if (hllElem.isActive()) {
          hllElem.merge(outCounter);
        }
      }
    } finally {
      row.endWrite();
      param.clear();
    }
  }
}
