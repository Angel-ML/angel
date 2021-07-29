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
package com.tencent.angel.graph.psf.hyperanf;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;

public class InitHyperLogLog extends UpdateFunc {

  public InitHyperLogLog(int matrixId, int p, int sp, long[] nodes, long seed) {
    super(new InitHyperLogLogParam(matrixId, p, sp, nodes, seed));
  }

  public InitHyperLogLog(InitHyperLogLogParam param) {
    super(param);
  }

  public InitHyperLogLog() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitHyperLogLogPartParam param = (InitHyperLogLogPartParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    // Get nodes and features
    ILongKeyPartOp split = (ILongKeyPartOp) param.getNodes();
    long[] nodes = split.getKeys();
    int p = param.getP();
    int sp = param.getSp();
    long seed = param.getSeed();

    row.startWrite();
    try {
      for (int i = 0; i < nodes.length; i++) {
        row.set(nodes[i], new HyperLogLogPlusElement(nodes[i], p, sp, seed));
      }
    } finally {
      row.endWrite();
    }

  }

}