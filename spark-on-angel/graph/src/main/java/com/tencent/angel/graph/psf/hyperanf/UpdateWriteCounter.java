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
package com.tencent.angel.graph.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;


public class UpdateWriteCounter extends UpdateFunc {

  public UpdateWriteCounter(UpdateWriteCounterParam param) {
    super(param);
  }

  public UpdateWriteCounter(int matrixId, Long2ObjectOpenHashMap<HyperLogLogPlus> updates, int p, int sp, long seed) {
    super(new UpdateWriteCounterParam(matrixId, updates, p, sp, seed));
  }

  public UpdateWriteCounter() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {

    UpdateWriteCounterPartParam param = (UpdateWriteCounterPartParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
    int p = param.getP();
    int sp = param.getSp();
    long seed = param.getSeed();
    long[] keys = split.getKeys();
    IElement[] values = split.getValues();

    row.startWrite();
    try {
      if (keys != null && keys.length > 0 && values != null && values.length > 0) {
        for (int i = 0; i < keys.length; i++) {
          long key = keys[i];
          HyperLogLogPlus value = ((HLLPlusElement) values[i]).getCounter();
          if (!row.exist(key))
            row.set(key, new HyperLogLogElement(key, p, sp, seed));
          HyperLogLogElement hllElem = (HyperLogLogElement) row.get(key);
          if (hllElem.isActive()) {
            hllElem.merge(value);
          }
        }
      }
    } finally {
      row.endWrite();
    }

  }

}