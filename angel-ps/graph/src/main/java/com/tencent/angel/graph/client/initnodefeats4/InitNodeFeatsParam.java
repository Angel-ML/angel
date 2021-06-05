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
package com.tencent.angel.graph.client.initnodefeats4;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.utils.LongIndexComparator;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.util.ArrayList;
import java.util.List;

public class InitNodeFeatsParam extends UpdateParam {

  private final long[] keys;
  private final IntFloatVector[] feats;
  private final int startIndex;
  private final int endIndex;

  public InitNodeFeatsParam(int matrixId, long[] keys, IntFloatVector[] feats, int start, int end) {
    super(matrixId);
    this.keys = keys;
    this.feats = feats;
    this.startIndex = start;
    this.endIndex = end;
    assert (keys.length == feats.length);
  }

  public InitNodeFeatsParam(int matrixId, long[] keys, IntFloatVector[] feats) {
    super(matrixId);
    this.keys = keys;
    this.feats = feats;
    this.startIndex = 0;
    this.endIndex = keys.length;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    LongIndexComparator comparator = new LongIndexComparator(keys);
    int size = endIndex - startIndex;
    int[] index = new int[size];
    for (int i = 0; i < size; i++) {
      index[i] = i + startIndex;
    }
    IntArrays.quickSort(index, comparator);

    List<PartitionUpdateParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(keys, index, parts)) {
      throw new AngelException(
          "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
              .get(parts.size() - 1).getEndCol());
    }

    int nodeIndex = startIndex;
    int partIndex = 0;
    while (nodeIndex < endIndex || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (nodeIndex < endIndex && keys[index[nodeIndex - startIndex]] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        params.add(new InitNodeFeatsPartParam(matrixId,
            parts.get(partIndex), keys, feats, index,
            nodeIndex - length - startIndex,
            nodeIndex - startIndex));
      }

      partIndex++;
    }

    return params;
  }

}
