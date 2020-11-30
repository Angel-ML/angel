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

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

import java.util.List;

public class GetCloseness extends GetFunc {

  public GetCloseness(int matrixId, long[] nodes, long n) {
    super(new GetHyperLogLogParam(matrixId, nodes, n, false));
  }

  public GetCloseness(GetParam param) {
    super(param);
  }

  public GetCloseness() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetHyperLogLogPartParam param = (GetHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long n = param.getN();
    long[] nodes = param.getNodes();
    Long2DoubleOpenHashMap closenesses = new Long2DoubleOpenHashMap();
    for (int i = 0; i < nodes.length; i++) {
      HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) row.get(nodes[i]);
      if (hllElem.getCloseness() < n - 1) {
        closenesses.put(nodes[i], 0);
      } else {
        closenesses.put(nodes[i], (double)n / (double)hllElem.getCloseness());
      }
    }
    return new GetClosenessPartResult(closenesses);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2DoubleOpenHashMap closenesses = new Long2DoubleOpenHashMap();
    for (PartitionGetResult r : partResults) {
      GetClosenessPartResult rr = (GetClosenessPartResult) r;
      closenesses.putAll(rr.getClosenesses());
    }
    return new GetClosenessResult(closenesses);
  }
}
