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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullpathtail;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

public class PullPathTailParam extends GetParam {

  private int partitionId;
  private int batchSize = -1;

  public PullPathTailParam(int matrixId, int partitionId, int batchSize) {
    super(matrixId);
    this.partitionId = partitionId;
    this.batchSize = batchSize;
  }

  public PullPathTailParam(int matrixId, int partitionId) {
    super(matrixId);
    this.partitionId = partitionId;
  }

  public PullPathTailParam(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(parts.size());

    for (PartitionKey part : parts) {
      partParams.add(new PullPathTailPartitionParam(matrixId, part, partitionId, batchSize));
    }

    return partParams;
  }
}
