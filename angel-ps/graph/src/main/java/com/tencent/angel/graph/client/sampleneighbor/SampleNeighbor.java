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

package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.CSRPartition;
import com.tencent.angel.ps.storage.partition.storage.IntCSRStorage;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
    CSRPartition part = (CSRPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
    IntCSRStorage storage = (IntCSRStorage) (part.getStorage());

    Int2ObjectOpenHashMap<int[]> results = new Int2ObjectOpenHashMap<>();
    int[] neighborOffsets = storage.getRowOffsets();
    int[] neighbors = storage.getColumnIndices();
    int startCol = (int) partParam.getPartKey().getStartCol();

    int[] nodeIds = param.getNodeIds();
    int count = param.getCount();
    Random r = new Random();

    for (int i = 0; i < nodeIds.length; i++) {
      int nodeId = nodeIds[i];

      // Get node neighbor number
      int num = neighborOffsets[nodeId - startCol + 1] - neighborOffsets[nodeId - startCol];
      int[] result;

      if (num == 0) {
        // If the neighbor number is 0, just return a int[0]
        result = new int[0];
      } else if (count <= 0 || num <= count) {
        // If count <= 0 or the neighbor number is less or equal then count, just copy all neighbors to the result array
        result = new int[num];
        System.arraycopy(neighbors, neighborOffsets[nodeId - startCol], result, 0, num);
      } else {
        // If the neighbor number > count, just copy a range of neighbors to the result array, the copy position is random
        int startPos = Math.abs(r.nextInt()) % num;
        result = new int[count];
        if (startPos + count <= num) {
          System.arraycopy(neighbors, neighborOffsets[nodeId - startCol] + startPos, result, 0,
              count);
        } else {
          System
              .arraycopy(neighbors, neighborOffsets[nodeId - startCol] + startPos, result, 0,
                  num - startPos);
          System.arraycopy(neighbors, neighborOffsets[nodeId - startCol], result,
              num - startPos, count - (num - startPos));
        }
      }

      results.put(nodeIds[i], result);
    }

    return new PartSampleNeighborResult(results);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int len = 0;
    for (PartitionGetResult result : partResults) {
      len += ((PartSampleNeighborResult) result).getNodeIdToNeighbors().size();
    }

    Int2ObjectOpenHashMap<int[]> nodeIdToNeighbors = new Int2ObjectOpenHashMap<>(len);
    for (PartitionGetResult result : partResults) {
      nodeIdToNeighbors.putAll(((PartSampleNeighborResult) result).getNodeIdToNeighbors());
    }

    return new SampleNeighborResult(nodeIdToNeighbors);
  }
}
