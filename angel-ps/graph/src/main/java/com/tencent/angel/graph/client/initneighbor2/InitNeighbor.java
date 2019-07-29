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

package com.tencent.angel.graph.client.initneighbor2;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Init node neighbors for long type node id
 */
public class InitNeighbor extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public InitNeighbor(InitNeighborParam param) {
    super(param);
  }

  public InitNeighbor() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNeighborParam param = (PartInitNeighborParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix
        .getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

    ObjectIterator<Long2ObjectMap.Entry<long[]>> iter = param
        .getNodeIdToNeighborIndices().long2ObjectEntrySet().iterator();

    row.startWrite();
    try {
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<long[]> entry = iter.next();
        row.set(entry.getLongKey(), new LongArrayElement(entry.getValue()));
      }
    } finally {
      row.endWrite();
    }
  }
}
