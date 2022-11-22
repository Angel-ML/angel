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

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;


/**
 * UpdateFunc for computing the cardinality and closeness without normalization, updating the
 * activeness status, and switching the read/write counter
 */
public class ComputeCloseness extends UpdateFunc {

  public ComputeCloseness(ComputeClosenessParam param) {
    super(param);
  }

  public ComputeCloseness(int matrixId, int r, boolean isConnected) {
    super(new ComputeClosenessParam(matrixId, r, isConnected));
  }

  public ComputeCloseness() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ComputeClosenessPartParam param = (ComputeClosenessPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(param.getPartKey(), 0);
    int r = param.getR();
    boolean isConnected = param.getIsConnected();

    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) iter.next().getValue();
      if (hllElem.isActive()) {
        hllElem.updateCloseness(r, isConnected);
      }
    }
  }
}