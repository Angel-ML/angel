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
package com.tencent.angel.graph.psf.gcn;

import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongFloatRow;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.util.List;

public class GetLabels extends GetFunc {
  private int matrixId;
  public GetLabels(GetLabelsParam param) {
    super(param);
    matrixId = param.getMatrixId();
  }

  public GetLabels(int matrixId, long[] nodes) {
    super(new GetLabelsParam(matrixId, nodes));
  }

  public GetLabels() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GetLabelsPartParam param = (GetLabelsPartParam) partParam;
    ServerLongFloatRow row = (ServerLongFloatRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);
    long[] nodes = param.getNodes();
    LongArrayList keys = new LongArrayList();
    FloatArrayList vals = new FloatArrayList();
    for (int i = 0; i < nodes.length; i++) {
      if (row.exist(nodes[i])) {
        keys.add(nodes[i]);
        vals.add(row.get(nodes[i]));
      }
    }

    return new GetLabelsPartResult(keys.toLongArray(), vals.toFloatArray());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int size = 0;
    for (PartitionGetResult result: partResults)
      size += ((GetLabelsPartResult) result).size();

    long dim = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId).getColNum();

    LongFloatVector vector = VFactory.sparseLongKeyFloatVector(dim, size);
    for (PartitionGetResult result: partResults) {
      GetLabelsPartResult r = (GetLabelsPartResult) result;
      long[] keys = r.getKeys();
      float[] vals = r.getValues();
      assert (keys.length == vals.length);
      for (int i = 0; i < keys.length; i++)
        vector.set(keys[i], vals[i]);
    }

    return new GetLabelsResult(vector);
  }
}
