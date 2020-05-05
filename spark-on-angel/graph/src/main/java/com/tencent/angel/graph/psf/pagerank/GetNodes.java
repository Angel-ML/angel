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
package com.tencent.angel.graph.psf.pagerank;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.IndexPartGetLongResult;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
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
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partParam.getPartKey());
    ServerRow ranks = psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 2);

    FloatVector ranksVector = ServerRowUtils.getVector((ServerLongFloatRow) ranks);

    long[] ret;
    if (ranksVector instanceof IntFloatVector)
      ret = gatherNodes((IntFloatVector) ranksVector, part.getPartitionKey().getStartCol());
    else if (ranksVector instanceof LongFloatVector)
      ret = gatherNodes((LongFloatVector) ranksVector, part.getPartitionKey().getStartCol());
    else {
      throw new AngelException("vector should be intfloat or longfloat but is " + ranksVector.getClass().getName());
    }

    return new IndexPartGetLongResult(part.getPartitionKey(), ret);
  }

  private long[] gatherNodes(IntFloatVector ranks, long offset) {
    if (ranks.getStorage().isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> it = ranks.getStorage().entryIterator();
      LongArrayList ret = new LongArrayList();
      while (it.hasNext()) {
        Int2FloatMap.Entry entry = it.next();
        int key = entry.getIntKey();
        ret.add(key + offset);
      }

      return ret.toLongArray();
    } else {
      float[] vals = ranks.getStorage().getValues();
      LongArrayList ret = new LongArrayList();
      for (int i = 0; i < vals.length; i++)
        if (vals[i] != 0.0)
          ret.add(i + offset);
      return ret.toLongArray();
    }
  }

  private long[] gatherNodes(LongFloatVector ranks, long offset) {
    if (ranks.getStorage().isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> it = ranks.getStorage().entryIterator();
      LongArrayList ret = new LongArrayList();
      while (it.hasNext()) {
        Long2FloatMap.Entry entry = it.next();
        long key = entry.getLongKey();
        ret.add(key + offset);
      }

      return ret.toLongArray();
    } else {
      float[] vals = ranks.getStorage().getValues();
      LongArrayList ret = new LongArrayList();
      for (int i = 0; i < vals.length; i++)
        if (vals[i] != 0.0)
          ret.add(i + offset);
      return ret.toLongArray();
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    LongArrayList longs = new LongArrayList();
    for (PartitionGetResult result: partResults) {
      if (result instanceof IndexPartGetLongResult) {
        long[] values = ((IndexPartGetLongResult) result).getValues();
        for (int i = 0; i < values.length; i++)
          longs.add(values[i]);

      }
    }

    return new GetRowResult(ResponseType.SUCCESS,
      VFactory.denseLongVector(longs.toLongArray()));
  }


}
