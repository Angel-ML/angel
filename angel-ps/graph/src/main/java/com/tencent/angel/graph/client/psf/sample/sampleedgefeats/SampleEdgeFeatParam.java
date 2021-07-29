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

package com.tencent.angel.graph.client.psf.sample.sampleedgefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class SampleEdgeFeatParam extends GetParam {

  /**
   * Node ids
   */
  protected final long[] nodeIds;

  /**
   * Sample number
   */
  private int count;

  public SampleEdgeFeatParam(int matrixId, long[] nodeIds, int count) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.count = count;
  }

  public SampleEdgeFeatParam() {
    this(-1, null, -1);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public int getCount() {
    return count;
  }

  @Override
  public List<PartitionGetParam> split() {
    // Get matrix meta
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] partitions = meta.getPartitionKeys();

    // Split nodeIds
    KeyPart[] splits = RouterUtils.split(meta, 0, nodeIds);
    assert partitions.length == splits.length;

    // Generate rpc params
    List<PartitionGetParam> partParams = new ArrayList<>(partitions.length);
    for (int i = 0; i < partitions.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new PartSampleEdgeFeatParam(matrixId, partitions[i], splits[i], count));
      }
    }

    return partParams;
  }
}
