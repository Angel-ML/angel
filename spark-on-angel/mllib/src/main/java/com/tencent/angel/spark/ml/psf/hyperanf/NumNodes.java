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

import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

import java.util.List;

public class NumNodes extends GetFunc {

  public NumNodes(int matrixId) {
    this(new GetParam(matrixId));
  }

  public NumNodes(GetParam param) {
    super(param);
  }

  public NumNodes() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partParam.getPartKey(), 0);
    return new ScalarPartitionAggrResult(row.size());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    long numNodes = 0;
    for (PartitionGetResult result : partResults) {
      if (result instanceof ScalarPartitionAggrResult) {
        long value = (long)((ScalarPartitionAggrResult) result).result;
        numNodes += value;
      }
    }
    return new NumNodesResult(numNodes);
  }
}
