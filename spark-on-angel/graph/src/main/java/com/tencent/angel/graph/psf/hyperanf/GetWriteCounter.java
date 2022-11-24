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
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import scala.Tuple2;

public class GetWriteCounter extends GetFunc {

  public GetWriteCounter(GetHyperLogLogParam param) {
    super(param);
  }

  public GetWriteCounter(int matrixId, long[] nodes) {
    super(new GetHyperLogLogParam(matrixId, nodes, 0L, false, false));
  }

  public GetWriteCounter() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetHyperLogLogPartParam param = (GetHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    ILongKeyPartOp keyPart = (ILongKeyPartOp) param.getNodes();
    long[] nodes = keyPart.getKeys();
    Long2ObjectOpenHashMap<Tuple2<HyperLogLogPlus, Long>> logs = new Long2ObjectOpenHashMap<>();
    row.startRead(20000);
    try {
      for (int i = 0; i < nodes.length; i++) {
        HyperLogLogElement hllElem = (HyperLogLogElement) row.get(nodes[i]);
        logs.put(nodes[i], new Tuple2<>(hllElem.getWriteCounter(), hllElem.getCardDiff()));
      }
    } finally {
      row.endRead();
    }
    return new GetWriteCounterPartResult(logs);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Tuple2<HyperLogLogPlus, Long>> logs = new Long2ObjectOpenHashMap<>();
    for (PartitionGetResult r : partResults) {
      GetWriteCounterPartResult rr = (GetWriteCounterPartResult) r;
      logs.putAll(rr.getLogs());
    }

    return new GetWriteCounterResult(logs);
  }
}
