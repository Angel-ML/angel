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

package com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.sampleRandomNeighborTable;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.math3.distribution.EnumeratedDistribution;
import org.apache.commons.math3.util.Pair;

/**
 * Sample the neighbor
 */
public class GetNeighborRandomTable extends GetFunc {

  public GetNeighborRandomTable(GetNeighborRandomTableParam param) {
    super(param);
  }

  public GetNeighborRandomTable() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetNeighborRandomTableParam param = (PartGetNeighborRandomTableParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    long[] nodeIds = param.getNodeIds();
    long[][] neighbors = new long[nodeIds.length][];

    int[] count = param.getCount();

    for (int i = 0; i < nodeIds.length; i++) {
      long nodeId = nodeIds[i];

      // Get node neighbor number
      NeighborsRandomTableElement element = (NeighborsRandomTableElement) (row.get(nodeId));
      if (element == null) {
        neighbors[i] = null;
      } else {
        long[] nodeNeighbors = element.getNeighborIds();
        if (nodeNeighbors == null || nodeNeighbors.length == 0 || count[i] <= 0) {
          neighbors[i] = null;
        } else {
          neighbors[i] = new long[count[i]];
          float[] weights = element.getWeights();

          // start sampling random by weights probability for count times
          ArrayList<Pair<Long, Double>> neighborsAndWeight = new ArrayList<Pair<Long, Double>>();
          for (int j = 0; j < nodeNeighbors.length; j++) {
            neighborsAndWeight.add(new Pair(nodeNeighbors[j], new Double(weights[j])));
          }
          EnumeratedDistribution<Long> dist = new EnumeratedDistribution<Long>(neighborsAndWeight);
          Long[] result = new Long[count[i]];
          dist.sample(count[i], result);

          for (int j = 0; j < count[i]; j++) {
            neighbors[i][j] = result[j];
          }
        }
      }
    }

    return new PartGetNeighborRandomTableResult(part.getPartitionKey().getPartitionId(), neighbors);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
        partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartGetNeighborRandomTableResult) result).getPartId(), result);
    }

    GetNeighborRandomTableParam param = (GetNeighborRandomTableParam) getParam();
    long[] nodeIds = param.getNodeIds();
    List<PartitionGetParam> partParams = param.getPartParams();

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (PartitionGetParam partParam : partParams) {
      int start = ((PartGetNeighborRandomTableParam) partParam).getStartIndex();
      int end = ((PartGetNeighborRandomTableParam) partParam).getEndIndex();
      PartGetNeighborRandomTableResult partResult = (PartGetNeighborRandomTableResult) (partIdToResultMap
          .get(partParam.getPartKey().getPartitionId()));
      long[][] results = partResult.getNodeIdToNeighbors();
      for (int i = start; i < end; i++) {
        nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
      }
    }

    return new GetNeighborRandomTableResult(nodeIdToNeighbors);
  }
}
