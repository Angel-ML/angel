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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.constent.Constent;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartSampleEdgeFeatResult extends PartitionGetResult {

  /**
   * Partition id
   */
  public int partId;

  /**
   * Node id to sample neighbors map
   */
  public Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors;

  /**
   * Node id to sample edge feat map
   */
  public Long2ObjectOpenHashMap<IntFloatVector[]> nodeIdToSampleEdgeFeats;

  public PartSampleEdgeFeatResult(int partId,
      Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<IntFloatVector[]> nodeIdToSampleEdgeFeats) {
    this.partId = partId;
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
    this.nodeIdToSampleEdgeFeats = nodeIdToSampleEdgeFeats;
  }

  public PartSampleEdgeFeatResult() {
    this(-1, null, null);
  }

  public Long2ObjectOpenHashMap<long[]> getNodeIdToSampleNeighbors() {
    return nodeIdToSampleNeighbors;
  }

  public void setNodeIdToSampleNeighbors(
      Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors) {
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
  }

  public Long2ObjectOpenHashMap<IntFloatVector[]> getNodeIdToSampleEdgeFeats() {
    return nodeIdToSampleEdgeFeats;
  }

  public void setNodeIdToSampleEdgeFeats(
      Long2ObjectOpenHashMap<IntFloatVector[]> nodeIdToSampleEdgeFeats) {
    this.nodeIdToSampleEdgeFeats = nodeIdToSampleEdgeFeats;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleNeighbors.size());
    for (Long2ObjectOpenHashMap.Entry<long[]> entry : nodeIdToSampleNeighbors
        .long2ObjectEntrySet()) {
      ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
      long[] sampleNeighbors = entry.getValue();
      if (sampleNeighbors == null) {
        ByteBufSerdeUtils.serializeLongs(output, Constent.emptyLongs);
      } else {
        ByteBufSerdeUtils.serializeLongs(output, sampleNeighbors);
      }
    }

    if (nodeIdToSampleEdgeFeats != null) {
      ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleEdgeFeats.size());

      for (Long2ObjectOpenHashMap.Entry<IntFloatVector[]> entry : nodeIdToSampleEdgeFeats
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        IntFloatVector[] sampleEdgeFeats = entry.getValue();
        if (sampleEdgeFeats == null) {
          ByteBufSerdeUtils.serializeIntFloatVectors(output, Constent.emptyIntFloatVectors);
        } else {
          ByteBufSerdeUtils.serializeIntFloatVectors(output, sampleEdgeFeats);
        }
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    nodeIdToSampleNeighbors = new Long2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      long nodeId = ByteBufSerdeUtils.deserializeLong(input);
      long[] sampleNeighbors = ByteBufSerdeUtils.deserializeLongs(input);
      nodeIdToSampleNeighbors.put(nodeId, sampleNeighbors);
    }

    int nodeTypeSize = ByteBufSerdeUtils.deserializeInt(input);
    if (nodeTypeSize > 0) {
      nodeIdToSampleEdgeFeats = new Long2ObjectOpenHashMap<>(nodeTypeSize);
      for (int i = 0; i < nodeTypeSize; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        IntFloatVector[] sampleEdgeFeats = ByteBufSerdeUtils.deserializeIntFloatVectors(input);
        nodeIdToSampleEdgeFeats.put(nodeId, sampleEdgeFeats);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (Long2ObjectOpenHashMap.Entry<long[]> entry : nodeIdToSampleNeighbors
        .long2ObjectEntrySet()) {
      len += ByteBufSerdeUtils.LONG_LENGTH;
      long[] sampleNeighbors = entry.getValue();
      if (sampleNeighbors == null) {
        len += ByteBufSerdeUtils.serializedLongsLen(Constent.emptyLongs);
      } else {
        len += ByteBufSerdeUtils.serializedLongsLen(sampleNeighbors);
      }
    }

    len += ByteBufSerdeUtils.INT_LENGTH;
    if (nodeIdToSampleEdgeFeats != null) {
      for (Long2ObjectOpenHashMap.Entry<IntFloatVector[]> entry : nodeIdToSampleEdgeFeats
          .long2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        IntFloatVector[] sampleEdgeFeats = entry.getValue();
        if (sampleEdgeFeats == null) {
          len += ByteBufSerdeUtils.serializedIntFloatVectorsLen(Constent.emptyIntFloatVectors);
        } else {
          len += ByteBufSerdeUtils.serializedIntFloatVectorsLen(sampleEdgeFeats);
        }
      }
    }

    return len;
  }
}
