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
package com.tencent.angel.spark.ml.psf.kcore;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import java.util.ArrayList;
import java.util.List;

public class GetCores extends GetFunc {

  public GetCores(int matrixId, long[] nodes, int threshold, int rowId) {
    this(new GetCoresParam(matrixId, nodes, threshold, rowId));
  }

  public GetCores(GetParam param) {
    super(param);
  }

  public GetCores() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetCoresParam param = (PartGetCoresParam) partParam;
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partParam.getPartKey());
//        ServerRow ranks = psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 2);
    ServerLongIntRow row = (ServerLongIntRow) (((RowBasedPartition) part).getRow(param.getRowId()));

    long[] nodeIds = param.getNodeIds();
    int threshold = param.getThreshold();
//        int[] cores = new int[nodeIds.length];
    ArrayList<Integer> cores = new ArrayList<>();
    ArrayList<Long> nodes = new ArrayList<>();

    for (int i = 0; i < nodeIds.length; i++) {
      long nodeId = nodeIds[i];
      int core = row.get(nodeId);
      if (core > threshold) {
        nodes.add(nodeId);
        cores.add(core);
      }
    }
    long[] n = new long[nodes.size()];
    for (int i = 0; i < nodes.size(); i++) {
      n[i] = nodes.get(i);
    }

    int[] c = new int[cores.size()];
    for (int i = 0; i < cores.size(); i++) {
      c[i] = cores.get(i);
    }
    return new PartGetCoresResult(part.getPartitionKey().getPartitionId(), n, c);

  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Long2ObjectArrayMap<>(
        partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartGetCoresResult) result).getPartId(), result);
    }

    GetCoresParam param = (GetCoresParam) getParam();
    List<PartitionGetParam> partParams = param.getPartParams();

    Long2IntOpenHashMap nodeIdToCores = new Long2IntOpenHashMap();

    for (PartitionGetParam partParam : partParams) {
      PartGetCoresResult partResult = (PartGetCoresResult) (partIdToResultMap
          .get(partParam.getPartKey().getPartitionId()));
      int[] cores = partResult.getCores();
      long[] nodes = partResult.getNodes();
      for (int i = 0; i < cores.length; i++) {
        nodeIdToCores.put(nodes[i], cores[i]);
      }
    }

    return new GetCoresResult(nodeIdToCores);
  }
}
