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
package com.tencent.angel.graph.client.node2vec.getfuncs.pullneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.params.GetParamWithKeyIds;
import com.tencent.angel.graph.client.node2vec.params.PartitionGetParamWithIds;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import java.util.Arrays;

public class PullNeighborParam extends GetParamWithKeyIds {

  public PullNeighborParam(int matrixId, long[] nodeIds) {
    super(matrixId, nodeIds);
    Arrays.sort(nodeIds);
  }

  public PullNeighborParam(long[] nodeIds) {
    super(nodeIds);
  }

  @Override
  protected PartitionGetParam getPartitionParam(PartitionKey part, int startIdx, int endIdx) {
    // int matrixId, PartitionKey partKey, long[] keyIds, int startIdx, int endIdx
    return new PartitionGetParamWithIds(matrixId, part, keyIds, startIdx, endIdx);
  }
}
