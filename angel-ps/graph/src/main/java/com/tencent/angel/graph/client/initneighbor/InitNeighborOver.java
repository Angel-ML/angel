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
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.CSRPartition;
import com.tencent.angel.ps.storage.partition.storage.IntCSRStorage;
import java.util.List;

/**
 * Notify PS that all node neighbors information is pushed. PS generate the final node neighbor
 * table in CSR format
 */
public class InitNeighborOver extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public InitNeighborOver(UpdateParam param) {
    super(param);
  }

  public InitNeighborOver() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam param) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(param.getMatrixId());
    CSRPartition part = (CSRPartition) matrix.getPartition(param.getPartKey().getPartitionId());
    IntCSRStorage storage = (IntCSRStorage) part.getStorage();
    int startCol = (int) param.getPartKey().getStartCol();

    synchronized (storage) {
      // No data in this partition
      if (storage.getTempRowIds() == null) {
        return;
      }

      // Get total neighbor number
      int[] rowOffsets = storage.getRowOffsets();
      int accumOffset = 0;
      for (int i = 0; i < rowOffsets.length - 1; i++) {
        int offset = rowOffsets[i];
        rowOffsets[i] = accumOffset;
        accumOffset += offset;
      }

      rowOffsets[rowOffsets.length - 1] = accumOffset;

      // Final matrix column indices: neighbors node ids
      int[] cloumnIndices = new int[accumOffset];

      // Write positions in cloumnIndices for nodes
      int[] copyOffsets = new int[rowOffsets.length - 1];
      System.arraycopy(rowOffsets, 0, copyOffsets, 0, rowOffsets.length - 1);

      List<int[]> tempRowIds = storage.getTempRowIds();
      List<int[]> tempRowLens = storage.getTempRowLens();
      List<int[]> tempColumnIndices = storage.getTempColumnIndices();

      // Copy all cached sub column indices to final column indices
      int size = tempRowIds.size();
      for (int i = 0; i < size; i++) {
        // Read position for a sub column indices
        int copyLen = 0;
        int[] subTempRowIds = tempRowIds.get(i);
        int[] subTempLens = tempRowLens.get(i);
        int[] subTempColumnIndices = tempColumnIndices.get(i);

        for (int j = 0; j < subTempRowIds.length; j++) {
          // Copy column indices for a node to final column indices
          System.arraycopy(subTempColumnIndices, copyLen, cloumnIndices,
              copyOffsets[subTempRowIds[j] - startCol],
              subTempLens[j]);

          // Update write position for this node in final column indices
          copyOffsets[subTempRowIds[j] - startCol] += subTempLens[j];

          // Update the read position in sub column indices
          copyLen += subTempLens[j];
        }
      }

      storage.setColumnIndices(cloumnIndices);

      // Clear all temp data
      storage.setTempRowIds(null);
      storage.setTempRowLens(null);
      storage.setTempColumnIndices(null);
    }
  }
}
