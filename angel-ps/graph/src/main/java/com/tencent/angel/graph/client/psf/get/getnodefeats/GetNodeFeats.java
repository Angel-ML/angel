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
package com.tencent.angel.graph.client.psf.get.getnodefeats;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetNodeAttrsParam;
import com.tencent.angel.graph.data.*;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import com.twitter.chill.ScalaKryoInstantiator;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class GetNodeFeats extends GetFunc {

  /**
   * Create a get node feats func
   *
   * @param param parameter of get udf
   */
  public GetNodeFeats(GetNodeAttrsParam param) {
    super(param);
  }

  public GetNodeFeats() {
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

        Long2ObjectOpenHashMap<ByteArrayElement> nodeId2Feats =
            new Long2ObjectOpenHashMap<>(nodeIds.length);
        FeatureKeyType keyType = null;

        for (long nodeId : nodeIds) {
          if (row.get(nodeId) == null) {
            // If node not exist, just skip
            continue;
          }
          GraphNode node = (GraphNode) (row.get(nodeId));
          ByteArrayElement feat = node.getFeats();
          if (feat != null) {
            nodeId2Feats.put(nodeId, feat);
            keyType = node.getKeyType();
          }
        }

        if (keyType == null && nodeId2Feats.isEmpty()) {
          keyType = FeatureKeyType.INT;
        }

        return new PartGetNodeFeatsResult(param.getPartKey().getPartitionId(), nodeId2Feats, keyType);
      }

      default: {
        // TODO: support String, Int, and Any type node id
        throw new InvalidParameterException("Unsupported index type " + keyPart.getKeyType());
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Feature> nodeId2Feats =
        new Long2ObjectOpenHashMap<>(((GetNodeAttrsParam) param).getNodeIds().length);

    for (PartitionGetResult partitionGetResult : partResults) {
      Long2ObjectOpenHashMap<ByteArrayElement> partnodeId2Feats =
          ((PartGetNodeFeatsResult) partitionGetResult).getNodeIdToFeats();
      FeatureKeyType keyType =  ((PartGetNodeFeatsResult) partitionGetResult).getKeyType();
      if (partnodeId2Feats != null) {
        for (Long2ObjectMap.Entry<ByteArrayElement> entry: partnodeId2Feats.long2ObjectEntrySet()) {
          if (entry.getValue() != null) {
            switch (keyType) {
              case INT:
                try {
                  nodeId2Feats.put(entry.getLongKey(), ScalaKryoInstantiator.defaultPool().fromBytes(entry.getValue().getData(), IntKeyFeature.class));
                } catch (Exception e) {
                 throw new AngelException("Failed to deserialize IntKeyFeature: " + e.getMessage() + ", nodeId: " + entry.getLongKey());
                }
                break;
              case LONG:
                try {
                  nodeId2Feats.put(entry.getLongKey(), ScalaKryoInstantiator.defaultPool().fromBytes(entry.getValue().getData(), LongKeyFeature.class));
                } catch (Exception e) {
                  throw new AngelException("Failed to deserialize LongKeyFeature: " + e.getMessage() + ", nodeId: " + entry.getLongKey());
                }
                break;
              default:
                throw new IllegalArgumentException("Unsupported keyType: " + keyType);
            }
          }
        }
      }
    }
    return new GetNodeFeatsResult(nodeId2Feats);
  }
}