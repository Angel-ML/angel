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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor;

import com.tencent.angel.graph.client.node2vec.params.PartitionGetParamWithIds;
import com.tencent.angel.graph.client.node2vec.utils.Merge;
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PullNeighbor extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public PullNeighbor(GetParam param) {
    super(param);
  }

  public PullNeighbor() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartitionGetParamWithIds pparam = (PartitionGetParamWithIds) partParam;
    long[] keyIds = pparam.getKeyIds();

    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(pparam.getPartKey(), 0);

    Long2ObjectOpenHashMap<long[]> partResult = new Long2ObjectOpenHashMap<long[]>(keyIds.length);
    boolean isTrunc = PathQueue.isIsTrunc();
    if (PathQueue.getKeepProba() < 1.0 - 1e-6 && PathQueue.getKeepProba() > 1e-6) {
      double selectThred = PathQueue.getThreshold() / (1 - PathQueue.getKeepProba());
      for (long keyId : keyIds) {
        LongArrayElement longArrayElement = (LongArrayElement) row.get(keyId);

        long[] neighs = longArrayElement.getData();

        if (isTrunc && neighs.length > selectThred) {
          partResult.put(keyId, Arrays.copyOf(neighs, (int) selectThred));
        } else {
          partResult.put(keyId, neighs);
        }
      }
    } else {
      for (long keyId : keyIds) {
        LongArrayElement longArrayElement = (LongArrayElement) row.get(keyId);
        long[] neighs = longArrayElement.getData();
        partResult.put(keyId, neighs);
      }
    }

    return new PullNeighborPartitionResult(partResult);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    List<Long2ObjectOpenHashMap<long[]>> maps = new ArrayList<>(partResults.size());

    for (PartitionGetResult partResult : partResults) {
      maps.add(((PullNeighborPartitionResult) partResult).getPartResult());
    }

    return new PullNeighborResult(Merge.mergeHadhMap(maps));
  }
}
