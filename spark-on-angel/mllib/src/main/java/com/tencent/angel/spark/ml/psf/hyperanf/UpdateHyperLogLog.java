/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
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
          row.set(outNode, new HyperLogLogPlusElement(outNode, param.getP(), param.getSp()));
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
