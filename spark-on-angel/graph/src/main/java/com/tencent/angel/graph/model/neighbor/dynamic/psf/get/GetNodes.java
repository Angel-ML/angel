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
package com.tencent.angel.graph.model.neighbor.dynamic.psf.get;

import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborElement;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;

public class GetNodes extends GetFunc {

  public GetNodes(int matrixId, int psPartId) {
    this(new GetNodesParam(matrixId, psPartId));
  }

  public GetNodes(GetNodesParam param) {
    super(param);
  }

  public GetNodes() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);
    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = row.getStorage().iterator();

    long[] nodes = new long[row.size()];
    int i = 0;
    while (it.hasNext()) {
      Long2ObjectMap.Entry<IElement> next = it.next();
      DynamicNeighborElement ele = (DynamicNeighborElement) next.getValue();
      if (ele != null) {
        nodes[i] = next.getLongKey();
        i += 1;
      }
    }
    return new PartGetNodesResult(nodes);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int size = 0;
    for (PartitionGetResult result: partResults) {
      size += ((PartGetNodesResult) result).getNodes().length;
    }
    long[] re = new long[size];
    for (PartitionGetResult result : partResults) {
      long[] partResult = ((PartGetNodesResult) result).getNodes();
      System.arraycopy(partResult, 0, re, 0, partResult.length);
    }
    return new GetNodesResult(re);
  }
}
