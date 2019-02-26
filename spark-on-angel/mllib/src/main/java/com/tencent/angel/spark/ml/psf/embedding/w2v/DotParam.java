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

package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DotParam extends GetParam {

  int seed;
  int partitionId;
  int model;
  int[][] sentences;

  public DotParam(int matrixId,
                  int seed,
                  int partitionId,
                  int model,
                  int[][] sentences) {
    super(matrixId);
    this.seed = seed;
    this.partitionId = partitionId;
    this.model = model;
    this.sentences = sentences;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionGetParam> params = new ArrayList<>();
    Iterator<PartitionKey> iterator = pkeys.iterator();
    while (iterator.hasNext()) {
      PartitionKey pkey = iterator.next();
      params.add(new DotPartitionParam(matrixId,
              seed,
              partitionId,
              model,
              pkey,
              sentences));
    }
    return params;
  }
}
