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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushpathtail;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PushPathTailParam extends UpdateParam {

  private Long2LongOpenHashMap pathTail;
  private long[] keyIds;

  public PushPathTailParam(int matrixId, boolean updateClock, Long2LongOpenHashMap pathTail) {
    super(matrixId, updateClock);
    this.pathTail = pathTail;

    this.keyIds = new long[pathTail.size()];
    LongIterator iter = pathTail.keySet().iterator();
    int i = 0;
    while (iter.hasNext()) {
      keyIds[i] = iter.nextLong();
      i++;
    }
    assert (i == this.keyIds.length);
    Arrays.sort(this.keyIds);
  }

  public PushPathTailParam(int matrixId, Long2LongOpenHashMap pathTail) {
    this(matrixId, false, pathTail);
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.size());

    if (!RowUpdateSplitUtils.isInRange(keyIds, parts)) {
      throw new AngelException(
          "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
              .get(parts.size() - 1).getEndCol());
    }

    int nodeIndex = 0;
    for (PartitionKey part : parts) {
      int start = nodeIndex;  // include start
      while (nodeIndex < keyIds.length && keyIds[nodeIndex] < part.getEndCol()) {
        nodeIndex++;
      }
      int end = nodeIndex;  // exclude end
      int sizePart = end - start;
      if (sizePart > 0) {
        partParams
            .add(new PushPathTailPartitionParam(matrixId, part, pathTail, keyIds, start, end));
      }
    }

    return partParams;
  }

}
