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

package com.tencent.angel.graph.client.psf.get.getnodes;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetNodesParam extends GetParam {

  private int[] partitionIds;
  private Boolean isHash = true;

  public GetNodesParam(int matrixId, int[] partitionIds) {
    super(matrixId);
    this.partitionIds = partitionIds;
  }

  public GetNodesParam() {
    super();
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    isHash = meta.isHash();

    List<PartitionGetParam> params = new ArrayList<>();

    Map<Integer, PartitionKey> pkeys = new HashMap<>();
    for (PartitionKey pkey : partitions) {
      pkeys.put(pkey.getPartitionId(), pkey);
    }

    for (int i = 0; i < partitionIds.length; i++) {
      params.add(new PartGetNodesParam(matrixId, pkeys.get(partitionIds[i]), isHash));
    }

    return params;
  }
}
