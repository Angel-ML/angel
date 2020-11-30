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

package com.tencent.angel.graph.client.sampleneighbor3;

import com.tencent.angel.graph.data.Node;
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
import java.util.List;
import java.util.Random;

/**
 * Sample the neighbor
 */
public class SampleNeighbor extends GetFunc {

  public SampleNeighbor(SampleNeighborParam param) {
    super(param);
  }

  public SampleNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborParam param = (PartSampleNeighborParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    long[] nodeIds = param.getNodeIds();
    long[][] neighbors = new long[nodeIds.length][];

    int count = param.getCount();
    Random r = new Random(System.currentTimeMillis());

    for (int i = 0; i < nodeIds.length; i++) {
      long nodeId = nodeIds[i];

      // Get node neighbor number
      Node element = (Node) (row.get(nodeId));
      if (element == null) {
        neighbors[i] = null;
      } else {
        long[] nodeNeighbors = element.getNeighbors();
        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          neighbors[i] = null;
        } else if (count <= 0 || nodeNeighbors.length <= count) {
          neighbors[i] = nodeNeighbors;
        } else {
          neighbors[i] = new long[count];
          // If the neighbor number > count, just copy a range of neighbors to the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos, neighbors[i], 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, neighbors[i], 0,
                    nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, neighbors[i],
                nodeNeighbors.length - startPos, count - (nodeNeighbors.length - startPos));
          }
        }
      }
    }

    return new PartSampleNeighborResult(part.getPartitionKey().getPartitionId(), neighbors);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
        partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartSampleNeighborResult) result).getPartId(), result);
    }

    SampleNeighborParam param = (SampleNeighborParam) getParam();
    long[] nodeIds = param.getNodeIds();
    List<PartitionGetParam> partParams = param.getPartParams();

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (PartitionGetParam partParam : partParams) {
      int start = ((PartSampleNeighborParam) partParam).getStartIndex();
      int end = ((PartSampleNeighborParam) partParam).getEndIndex();
      PartSampleNeighborResult partResult = (PartSampleNeighborResult) (partIdToResultMap
          .get(partParam.getPartKey().getPartitionId()));
      long[][] results = partResult.getNodeIdToNeighbors();
      for (int i = start; i < end; i++) {
        nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
      }
    }

    return new SampleNeighborResult(nodeIdToNeighbors);
  }
}
