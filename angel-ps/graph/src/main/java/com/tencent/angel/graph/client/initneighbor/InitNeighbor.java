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

package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.CSRPartition;
import com.tencent.angel.ps.storage.partition.storage.IntCSRStorage;
import java.util.ArrayList;
import java.util.List;

/**
 * Init node neighbors
 */
public class InitNeighbor extends UpdateFunc {

  public InitNeighbor(InitNeighborParam param) {
    super(param);
  }

  public InitNeighbor() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNeighborParam param = (PartInitNeighborParam) partParam;
    int[] nodeIds = param.getNodeIds();
    int[] neighborNums = param.getNeighborNums();
    int[] neighbors = param.getNeighbors();

    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(param.getMatrixId());
    CSRPartition part = (CSRPartition) matrix.getPartition(param.getPartKey().getPartitionId());
    IntCSRStorage storage = (IntCSRStorage) part.getStorage();

    synchronized (storage) {
      int startOffset = (int) param.getPartKey().getStartCol();

      // Store the total neighbor number of all nodes in rowOffsets
      int[] rowOffsets = storage.getRowOffsets();
      for (int i = 0; i < nodeIds.length; i++) {
        rowOffsets[nodeIds[i] - startOffset] += neighborNums[i];
      }

      // Put the node ids, node neighbor number, node neighbors to the cache
      List<int[]> tempRowIds = storage.getTempRowIds();
      List<int[]> tempRowLens = storage.getTempRowLens();
      List<int[]> tempColumnOffsets = storage.getTempColumnIndices();
      if (tempRowIds == null) {
        tempRowIds = new ArrayList<>();
        tempRowLens = new ArrayList<>();
        tempColumnOffsets = new ArrayList<>();
        storage.setTempRowIds(tempRowIds);
        storage.setTempRowLens(tempRowLens);
        storage.setTempColumnIndices(tempColumnOffsets);
      }

      tempRowIds.add(param.getNodeIds());
      tempRowLens.add(neighborNums);
      tempColumnOffsets.add(neighbors);
    }
  }
}
