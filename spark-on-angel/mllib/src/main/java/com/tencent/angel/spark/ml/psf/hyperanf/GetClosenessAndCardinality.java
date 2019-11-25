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

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple3;

import java.util.List;

public class GetClosenessAndCardinality extends GetFunc {

  public GetClosenessAndCardinality(int matrixId, long[] nodes, long n, boolean isDirected) {
    super(new GetHyperLogLogParam(matrixId, nodes, n, isDirected));
  }

  public GetClosenessAndCardinality(GetParam param) {
    super(param);
  }

  public GetClosenessAndCardinality() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetHyperLogLogPartParam param = (GetHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long n = param.getN();
    long[] nodes = param.getNodes();
    boolean isDirected = param.isDirected();

    Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses = new Long2ObjectOpenHashMap<>();
    for (int i = 0; i < nodes.length; i++) {
      HyperLogLogPlusElement hllElem = (HyperLogLogPlusElement) row.get(nodes[i]);
      if (isDirected) {
        if (hllElem.getCloseness() < n) {
          closenesses.put(nodes[i], new Tuple3<>(0d, hllElem.getCardinality(), hllElem.getCloseness()));
        } else {
          closenesses.put(nodes[i], new Tuple3<>(((double) n / (double) hllElem.getCloseness()),
              hllElem.getCardinality(),
              hllElem.getCloseness()));
        }
      } else {
        closenesses.put(nodes[i], new Tuple3<>(((double) hllElem.getCardinality() / (double) hllElem.getCloseness()),
            hllElem.getCardinality(),
            hllElem.getCloseness()));
      }
    }
    return new GetClosenessAndCardinalityPartResult(closenesses);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Tuple3<Double, Long, Long>> closenesses = new Long2ObjectOpenHashMap<>();
    for (PartitionGetResult r : partResults) {
      GetClosenessAndCardinalityPartResult rr = (GetClosenessAndCardinalityPartResult) r;
      closenesses.putAll(rr.getClosenesses());
    }
    return new GetClosenessAndCardinalityResult(closenesses);
  }
}
