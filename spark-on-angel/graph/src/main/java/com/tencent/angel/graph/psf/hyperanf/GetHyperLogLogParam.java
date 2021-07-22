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
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class GetHyperLogLogParam extends GetParam {

  private long[] nodes;
  private long n;
  private boolean isDirected;

  public GetHyperLogLogParam(int matrixId, long[] nodes, long n, boolean isDirected) {
    super(matrixId);
    this.nodes = nodes;
    this.n = n;
    this.isDirected = isDirected;
  }

  @Override
  public List<PartitionGetParam> split() {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = matrixMeta.getPartitionKeys();
    KeyPart[] keyParts = RouterUtils.split(matrixMeta, 0, nodes);
    List<PartitionGetParam> params = new ArrayList<>(parts.length);

    for (int i = 0; i < parts.length; i++) {
      if (keyParts[i] != null && keyParts[i].size() > 0) {
        params.add(new GetHyperLogLogPartParam(matrixId, parts[i], keyParts[i], n, isDirected));
      }
    }

    return params;
  }
}
