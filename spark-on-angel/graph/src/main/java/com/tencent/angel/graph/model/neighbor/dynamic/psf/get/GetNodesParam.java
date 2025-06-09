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
package com.tencent.angel.graph.model.neighbor.dynamic.psf.get;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class GetNodesParam extends GetParam {

  private final int psPartId;

  public GetNodesParam(int matrixId, int psPartId) {
    super(matrixId);
    this.psPartId = psPartId;
  }

  public GetNodesParam() {
    this(-1, 0);
  }

  public int getPsPartId() {
    return psPartId;
  }

  @Override
  public List<PartitionGetParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();
    List<PartitionGetParam> partParams = new ArrayList<>(1);
    partParams.add(new PartitionGetParam(matrixId, parts[psPartId]));
    return partParams;
  }

}
