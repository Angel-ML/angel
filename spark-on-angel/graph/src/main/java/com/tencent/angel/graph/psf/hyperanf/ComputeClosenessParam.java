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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class ComputeClosenessParam extends UpdateParam {

  private int r;
  private boolean isConnected;

  public ComputeClosenessParam(int matrixId, int r, boolean isConnected) {
    super(matrixId);
    this.r = r;
    this.isConnected = isConnected;
  }

  @Override
  public List<PartitionUpdateParam> split() {

    List<PartitionUpdateParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    for (PartitionKey key : parts) {
      params.add(new ComputeClosenessPartParam(matrixId, key, r, isConnected));
    }

    return params;
  }
}
