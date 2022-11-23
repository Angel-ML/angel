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
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class InitHyperVertexParam extends UpdateParam {

  private long[] nodes;
  private int[] nodeTags;
  private int p;
  private int sp;
  private long seed;

  public InitHyperVertexParam(int matrixId, int p, int sp, long[] nodes, int[] nodeTags, long seed) {
    super(matrixId);
    this.p = p;
    this.sp = sp;
    this.nodes = nodes;
    this.nodeTags = nodeTags;
    this.seed = seed;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] splits = RouterUtils.split(meta, 0, nodes, nodeTags, false);
    assert parts.length == splits.length;

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new InitHyperVertexPartParam(matrixId, parts[i], splits[i], p, sp, seed));
      }
    }

    return partParams;
  }

}