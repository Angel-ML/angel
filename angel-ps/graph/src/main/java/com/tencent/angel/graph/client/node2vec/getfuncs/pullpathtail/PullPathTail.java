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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail;

import com.tencent.angel.graph.client.node2vec.utils.Merge;
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.List;

public class PullPathTail extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public PullPathTail(GetParam param) {
    super(param);
  }

  public PullPathTail() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PullPathTailPartitionParam pparam = (PullPathTailPartitionParam) partParam;
    int psPartitionId = pparam.getPartKey().getPartitionId();

    Long2ObjectOpenHashMap<long[]> result = new Long2ObjectOpenHashMap<>();
    PathQueue.popBatchTail(psPartitionId, pparam.getBatchSize(), result);

    return new PullPathTailPartitionResult(result);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<Long2ObjectOpenHashMap<long[]>> maps = new ArrayList<>(partResults.size());

    for (PartitionGetResult partResult : partResults) {
      if (partResult != null) {
        maps.add(((PullPathTailPartitionResult) partResult).getPartRestlt());
      }
    }

    return new PullPathTailResult(Merge.mergeHadhMap(maps));
  }
}
