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
package com.tencent.angel.graph.client.psf.get.getedgeweights;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetFloatArrayAttrsResult;
import com.tencent.angel.graph.client.psf.get.utils.GetNodeAttrsParam;
import com.tencent.angel.graph.client.psf.get.utils.PartGetFloatArrayAttrsResult;
import com.tencent.angel.graph.data.GraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class GetEdgeWeights extends GetFunc {

  /**
   * Create a get node feats func
   *
   * @param param parameter of get udf
   */
  public GetEdgeWeights(GetNodeAttrsParam param) {
    super(param);
  }

  public GetEdgeWeights() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GeneralPartGetParam param = (GeneralPartGetParam) partParam;
    KeyPart keyPart = param.getIndicesPart();

    switch (keyPart.getKeyType()) {
      case LONG: {
        // Long type node id
        long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        Long2ObjectOpenHashMap<float[]> nodeIdToWeights =
                new Long2ObjectOpenHashMap<>(nodeIds.length);
        for (long nodeId : nodeIds) {
          if (row.get(nodeId) == null) {
            // If node not exist, just skip
            continue;
          }
          float[] weights = ((GraphNode) (row.get(nodeId))).getWeights();
          if (weights != null) {
            nodeIdToWeights.put(nodeId, weights);
          }
        }
        return new PartGetFloatArrayAttrsResult(param.getPartKey().getPartitionId(), nodeIdToWeights);
      }

      default: {
        // TODO: support String, Int, and Any type node id
        throw new InvalidParameterException("Unsupport index type " + keyPart.getKeyType());
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<float[]> nodeIdToWeights =
            new Long2ObjectOpenHashMap<>(((GetNodeAttrsParam) param).getNodeIds().length);
    for (PartitionGetResult partitionGetResult : partResults) {
      Long2ObjectOpenHashMap<float[]> partNodeIdToWeights =
              ((PartGetFloatArrayAttrsResult) partitionGetResult).getNodeIdToContents();
      if (partNodeIdToWeights != null) {
        nodeIdToWeights.putAll(partNodeIdToWeights);
      }
    }
    return new GetFloatArrayAttrsResult(nodeIdToWeights);
  }
}