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

package com.tencent.angel.graph.model.neighbor.dynamic.psf.init;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborElement;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Init node neighbors for long type node id
 */
public class GetSort extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public GetSort(UpdateParam param) { super(param); }

  public GetSort() { this(null); }

  public static class GetSortParam extends UpdateParam {
    /**
     * Instantiates a new Zero updater param.
     *
     * @param matrixId    the matrix id
     * @param updateClock the update clock
     */
    public GetSortParam(int matrixId, boolean updateClock) {
      super(matrixId, updateClock);
    }

    @Override public List<PartitionUpdateParam> split() {
      List<PartitionKey> partList =
              PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();
      List<PartitionUpdateParam> partParamList = new ArrayList<PartitionUpdateParam>(size);
      for (int i = 0; i < size; i++) {
        partParamList.add(new GetSort.GetSortPartitionParam(matrixId, partList.get(i), updateClock));
      }

      return partParamList;
    }
  }

  public static class GetSortPartitionParam extends PartitionUpdateParam {
    /**
     * Creates new partition updater parameter.
     *
     * @param matrixId    the matrix id
     * @param partKey     the part key
     * @param updateClock the update clock
     */
    public GetSortPartitionParam(int matrixId, PartitionKey partKey, boolean updateClock) {
      super(matrixId, partKey, updateClock);
    }

    /**
     * Creates a new partition updater parameter by default.
     */
    public GetSortPartitionParam() {
      super();
    }
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
            .getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);
    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = row.getStorage().iterator();

    row.startWrite();
    try {
      while (it.hasNext()) {
        Long2ObjectMap.Entry<IElement> next = it.next();
        DynamicNeighborElement ele = (DynamicNeighborElement) next.getValue();
        if (ele != null) {
          ele.trans();
        }
      }
    } finally {
      row.endWrite();
    }
  }
}
