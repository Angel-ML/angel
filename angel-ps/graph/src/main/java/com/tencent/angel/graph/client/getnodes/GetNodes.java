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
package com.tencent.angel.graph.client.getnodes;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexPartGetLongResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;

public class GetNodes extends GetFunc {

  public GetNodes(int matrixId, int[] partitionIds) {
    this(new GetNodesParam(matrixId, partitionIds));
  }

  public GetNodes(GetParam param) {
    super(param);
  }

  public GetNodes() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam param) {
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = row.iterator();
    LongArrayList nodes = new LongArrayList();
    long start = param.getPartKey().getStartCol();
    while (it.hasNext()) {
      Long2ObjectMap.Entry entry = it.next();
      Node node = (Node) entry.getValue();
      if (node.getFeats() != null && node.getNeighbors() == null)
        nodes.add(entry.getLongKey() + start);
    }

    return new IndexPartGetLongResult(param.getPartKey(), nodes.toLongArray());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int size = 0;
    for (PartitionGetResult result: partResults) {
      if (result instanceof IndexPartGetLongResult) {
        size += ((IndexPartGetLongResult) result).getValues().length;
      }
    }

    long[] values = new long[size];
    int start = 0;
    for (PartitionGetResult result: partResults) {
      if (result instanceof IndexPartGetLongResult) {
        long[] vals = ((IndexPartGetLongResult) result).getValues();
        System.arraycopy(vals, 0, values, start, vals.length);
        start += vals.length;
      }
    }

    return new GetRowResult(ResponseType.SUCCESS,
      VFactory.denseLongVector(values));
  }
}
