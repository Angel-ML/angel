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

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitHyperLogLog extends UpdateFunc {

  public InitHyperLogLog(int matrixId, int p, int sp, long[] nodes) {
    super(new InitHyperLogLogParam(matrixId, p, sp, nodes));
  }

  public InitHyperLogLog(InitHyperLogLogParam param) {
    super(param);
  }

  public InitHyperLogLog() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParm) {
    InitHyperLogLogPartParam param = (InitHyperLogLogPartParam) partParm;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    int p = param.getP();
    int sp = param.getSp();
    long[] nodes = param.getNodes();
    row.startWrite();
    try {
      for (int i = 0; i < nodes.length; i++)
        row.set(nodes[i], new HyperLogLogPlusElement(nodes[i], p, sp));
    } finally {
      row.endWrite();
    }

  }

}
