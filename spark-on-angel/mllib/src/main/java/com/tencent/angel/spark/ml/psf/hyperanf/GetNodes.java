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
package com.tencent.angel.spark.ml.psf.hyperanf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexPartGetLongResult;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.ResponseType;
import com.tencent.angel.spark.ml.psf.pagerank.GetNodesParam;
import it.unimi.dsi.fastutil.longs.LongArrayList;

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
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partParam.getPartKey());
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);
    LongArrayList ret = new LongArrayList();
    for (long node = partParam.getPartKey().getStartCol(); node < partParam.getPartKey().getEndCol(); node++) {
      if (row.exist(node)) {
        ret.add(node);
      }
    }
    return new IndexPartGetLongResult(part.getPartitionKey(), ret.toLongArray());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    LongArrayList ret = new LongArrayList();
    for (PartitionGetResult result : partResults) {
      if (result instanceof IndexPartGetLongResult) {
        long[] values = ((IndexPartGetLongResult) result).getValues();
        for (int i = 0; i < values.length; i++)
          ret.add(values[i]);
      }
    }
    return new GetRowResult(ResponseType.SUCCESS,
        VFactory.denseLongVector(ret.toLongArray()));
  }
}
