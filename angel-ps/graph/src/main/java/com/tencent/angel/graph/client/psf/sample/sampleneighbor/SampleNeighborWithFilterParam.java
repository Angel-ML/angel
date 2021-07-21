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

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class SampleNeighborWithFilterParam extends SampleNeighborWithTypeParam {

  protected final long[] filterWithNeighKeys;
  protected final long[] filterWithoutNeighKeys;

  public SampleNeighborWithFilterParam(int matrixId, long[] nodeIds, int count,
      SampleType sampleType, long[] filterWithNeighKeys, long[] filterWithoutNeighKeys) {
    super(matrixId, nodeIds, count, sampleType);
    this.filterWithNeighKeys = filterWithNeighKeys;
    this.filterWithoutNeighKeys = filterWithoutNeighKeys;
  }

  public SampleNeighborWithFilterParam() {
    this(-1, null, -1, null, null, null);
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, filterWithNeighKeys);
    assert partitions.length == splits.length;

    // Generate node ids
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(
            new PartSampleNeighborWithFilterParam(matrixId, partitions[i], count, sampleType,
                splits[i], filterWithoutNeighKeys));
      }
    }

    return partParams;
  }

  public long[] getFilterWithNeighKeys() {
    return filterWithNeighKeys;
  }

  public long[] getFilterWithoutNeighKeys() {
    return filterWithoutNeighKeys;
  }
}
