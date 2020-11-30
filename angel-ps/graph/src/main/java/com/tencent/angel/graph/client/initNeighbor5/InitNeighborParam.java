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
package com.tencent.angel.graph.client.initNeighbor5;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.utils.LongIndexComparator;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.ints.IntArrays;

import java.util.ArrayList;
import java.util.List;

public class InitNeighborParam extends UpdateParam {

  private long[] keys;
  private int[] indptr;
  private long[] neighbors;
  private int[] types;
  private int start;
  private int end;

  public InitNeighborParam(int matrixId, long[] keys,
                           int[] indptr, long[] neighbors) {
    this(matrixId, keys, indptr, neighbors, 0, keys.length);
  }

  public InitNeighborParam(int matrixId, long[] keys,
                           int[] indptr, long[] neighbors,
                           int start, int end) {
    this(matrixId, keys, indptr, neighbors, null, start, end);
  }

  public InitNeighborParam(int matrixId, long[] keys,
                           int[] indptr, long[] neighbors,
                           int[] types,
                           int start, int end) {
    super(matrixId);
    this.keys = keys;
    this.indptr = indptr;
    this.neighbors = neighbors;
    this.types = types;
    this.start = start;
    this.end = end;
    assert start > 0 && start < end && end < keys.length;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    LongIndexComparator comparator = new LongIndexComparator(keys);
    int size = end - start;
    int[] index = new int[size];
    for (int i = 0; i < size; i++)
      index[i] = i + start;
    IntArrays.quickSort(index, comparator);

    List<PartitionUpdateParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(keys, index, parts)) {
      throw new AngelException(
        "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
          .get(parts.size() - 1).getEndCol());
    }

    int nodeIndex = start;
    int partIndex = 0;
    while (nodeIndex < end || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (nodeIndex < end && keys[index[nodeIndex - start]] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0)
        params.add(new InitNeighborPartParam(matrixId,
          parts.get(partIndex), keys, index, indptr, neighbors, types,
          nodeIndex - length - start, nodeIndex - start));

      partIndex++;
    }

    return params;
  }
}
