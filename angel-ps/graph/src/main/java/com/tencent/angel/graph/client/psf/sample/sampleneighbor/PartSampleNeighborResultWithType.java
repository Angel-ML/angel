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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.client.constent.Constent;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Result of GetNeighbor with type type: node type or edge type
 */
public class PartSampleNeighborResultWithType extends PartSampleNeighborResult {

  /**
   * Node id to neighbors type map
   */
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType;

  /**
   * Node id to edge type map
   */
  private Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType;


  public PartSampleNeighborResultWithType(int partId,
      Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToType, SampleType sampleType) {
    super(partId, nodeIdToSampleNeighbors);
    if (sampleType == SampleType.NODE) {
      this.nodeIdToSampleNeighborsType = nodeIdToType;
    } else if (sampleType == SampleType.EDGE) {
      this.nodeIdToSampleEdgeType = nodeIdToType;
    }
  }


  public PartSampleNeighborResultWithType(int partId,
      Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType,
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType) {
    super(partId, nodeIdToSampleNeighbors);
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
  }

  public PartSampleNeighborResultWithType() {
    this(-1, null, null, SampleType.SIMPLE);
  }

  public Long2ObjectOpenHashMap<long[]> getNodeIdToSampleNeighbors() {
    return nodeIdToSampleNeighbors;
  }

  public void setNodeIdToSampleNeighbors(Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors) {
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
  }

  public Long2ObjectOpenHashMap<int[]> getNodeIdToSampleNeighborsType() {
    return nodeIdToSampleNeighborsType;
  }

  public void setNodeIdToSampleNeighborsType(
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborsType) {
    this.nodeIdToSampleNeighborsType = nodeIdToSampleNeighborsType;
  }

  public Long2ObjectOpenHashMap<int[]> getNodeIdToSampleEdgeType() {
    return nodeIdToSampleEdgeType;
  }

  public void setNodeIdToSampleEdgeType(
      Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType) {
    this.nodeIdToSampleEdgeType = nodeIdToSampleEdgeType;
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

    if (nodeIdToSampleNeighborsType != null) {
      ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleNeighborsType.size());

      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleNeighborsType
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        int[] sampleNeighborsType = entry.getValue();
        if (sampleNeighborsType == null) {
          ByteBufSerdeUtils.serializeInts(output, Constent.emptyInts);
        } else {
          ByteBufSerdeUtils.serializeInts(output, sampleNeighborsType);
        }
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }

    if (nodeIdToSampleEdgeType != null) {
      ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleEdgeType.size());
      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleEdgeType
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        int[] sampleEdgeType = entry.getValue();
        if (sampleEdgeType == null) {
          ByteBufSerdeUtils.serializeInts(output, Constent.emptyInts);
        } else {
          ByteBufSerdeUtils.serializeInts(output, sampleEdgeType);
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
      nodeIdToSampleNeighborsType = new Long2ObjectOpenHashMap<>(nodeTypeSize);
      for (int i = 0; i < nodeTypeSize; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        int[] sampleNeighborsType = ByteBufSerdeUtils.deserializeInts(input);
        nodeIdToSampleNeighborsType.put(nodeId, sampleNeighborsType);
      }
    }

    int nodeEdgeSize = ByteBufSerdeUtils.deserializeInt(input);
    if (nodeEdgeSize > 0) {
      nodeIdToSampleEdgeType = new Long2ObjectOpenHashMap<>(nodeEdgeSize);
      for (int i = 0; i < nodeEdgeSize; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        int[] sampleEdgeType = ByteBufSerdeUtils.deserializeInts(input);
        nodeIdToSampleEdgeType.put(nodeId, sampleEdgeType);
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
    if (nodeIdToSampleNeighborsType != null) {
      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleNeighborsType
          .long2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        int[] sampleNeighborsType = entry.getValue();
        if (sampleNeighborsType == null) {
          len += ByteBufSerdeUtils.serializedIntsLen(Constent.emptyInts);
        } else {
          len += ByteBufSerdeUtils.serializedIntsLen(sampleNeighborsType);
        }
      }
    }

    len += ByteBufSerdeUtils.INT_LENGTH;
    if (nodeIdToSampleEdgeType != null) {
      for (Long2ObjectOpenHashMap.Entry<int[]> entry : nodeIdToSampleEdgeType
          .long2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.LONG_LENGTH;
        int[] sampleEdgeType = entry.getValue();
        if (sampleEdgeType == null) {
          len += ByteBufSerdeUtils.serializedIntsLen(Constent.emptyInts);
        } else {
          len += ByteBufSerdeUtils.serializedIntsLen(sampleEdgeType);
        }
      }
    }
    return len;
  }
}
