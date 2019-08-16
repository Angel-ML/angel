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
package com.tencent.angel.spark.ml.psf.pagerank;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MyPullParam extends GetParam {
  private long[] keys;
  private int deltaId;
  private int sumId;
  private float resetProb;
  private float tol;

  public MyPullParam(int matrixId, int deltaId, int sumId,
                     float resetProb, float tol, long[] keys) {
    super(matrixId);
    this.deltaId = deltaId;
    this.sumId = sumId;
    this.keys = keys;
    this.resetProb = resetProb;
    this.tol = tol;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(keys);
    List<PartitionGetParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);


    if (!RowUpdateSplitUtils.isInRange(keys, parts)) {
      throw new AngelException(
        "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
          .get(parts.size() - 1).getEndCol());
    }

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < keys.length || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (nodeIndex < keys.length && keys[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0)
        params.add(new MyPullPartParam(matrixId, parts.get(partIndex),
          keys, deltaId, sumId, resetProb, tol,
          nodeIndex - length, nodeIndex));

      partIndex++;
    }
    return params;
  }
}
