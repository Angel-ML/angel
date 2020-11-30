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
package com.tencent.angel.graph.client.node2vec.getfuncs.getmindegree;

import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import com.tencent.angel.graph.client.node2vec.getfuncs.getmaxdegree.PullMaxDegreePartitionResult;
import com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail.PullPathTailPartitionParam;
import com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail.PullPathTailPartitionResult;
import com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail.PullPathTailResult;
import com.tencent.angel.graph.client.node2vec.utils.Merge;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import com.tencent.angel.ps.storage.vector.storage.LongIntVectorStorage;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.ArrayList;
import java.util.List;

public class PullMinDegree extends GetFunc {
  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public PullMinDegree(GetParam param) {
    super(param);
  }

  public PullMinDegree() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);

    int partResult = Integer.MAX_VALUE;
    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = row.iterator();
    while (iter.hasNext()) {
      Long2ObjectMap.Entry<IElement> entry = iter.next();
      LongArrayElement value = (LongArrayElement) entry.getValue();
      int length = value.getData().length;

      if (length < partResult) {
        partResult = length;
      }
    }

    return new PullMinDegreePartitionResult(partResult);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int result = Integer.MAX_VALUE;

    for (PartitionGetResult partResult : partResults) {
      int part = ((PullMinDegreePartitionResult) partResult).getPartResult();
      if (part < result) {
        result = part;
      }
    }

    return new PullMinDegreeResult(result);
  }
}
