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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class InitHyperLogLogParam extends UpdateParam {

  private long[] nodes;
  private int p;
  private int sp;

  public InitHyperLogLogParam(int matrixId, int p, int sp, long[] nodes) {
    super(matrixId);
    this.p = p;
    this.sp = sp;
    this.nodes = nodes;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = matrixMeta.getPartitionKeys();
    KeyPart[] keyParts = RouterUtils.split(matrixMeta, 0, nodes);
    List<PartitionUpdateParam> params = new ArrayList<>(parts.length);

    for (int i = 0; i < parts.length; i++) {
      if (keyParts[i] != null && keyParts[i].size() > 0) {
        params.add(new InitHyperLogLogPartParam(matrixId, parts[i], keyParts[i], p, sp));
      }
    }

    return params;
  }

}
