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
package com.tencent.angel.graph.client.sampleneighbor4;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SampleNeighborParam extends GetParam {

  private final long[] keys;
  private final int numSample;
  private final List<PartitionGetParam> params;
  private final boolean sampleTypes;

  public SampleNeighborParam(int matrixId, long[] keys, int numSample, boolean sampleTypes) {
    super(matrixId);
    this.keys = keys;
    this.numSample = numSample;
    this.params = new ArrayList<>();
    this.sampleTypes = sampleTypes;
  }

  public SampleNeighborParam() {
    this(-1, null, -1, false);
  }

  public List<PartitionGetParam> getParams() {
    return params;
  }

  public long[] getKeys() {
    return keys;
  }

  public boolean getSampleTypes() {
    return sampleTypes;
  }

  @Override
  public List<PartitionGetParam> split() {
    Arrays.sort(keys);
    List<PartitionKey> partitions =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(keys, partitions)) {
      throw new AngelException(
          "node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
              .get(partitions.size() - 1).getEndCol());
    }

    int nodeIndex = 0;
    int partIndex = 0;
    while (nodeIndex < keys.length || partIndex < partitions.size()) {
      int length = 0;
      long endOffset = partitions.get(partIndex).getEndCol();
      while (nodeIndex < keys.length && keys[nodeIndex] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        params.add(new SampleNeighborPartParam(matrixId,
            partitions.get(partIndex), numSample, keys, sampleTypes,
            nodeIndex - length, nodeIndex));
      }

      partIndex++;
    }

    return params;
  }


}
