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
import com.tencent.angel.graph.data.IntNeighbor;
import com.tencent.angel.graph.data.LongNeighbor;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

/**
 * Result of GetNeighbor
 */
public class PartSampleNeighborResult extends PartitionGetResult {

  /**
   * Partition id
   */
  public int partId;

  /**
   * Node id to sample neighbors map
   */
  public Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors;
  public NeighborStorageType storageType;

  public PartSampleNeighborResult(int partId,
      Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors, NeighborStorageType storageType) {
    this.partId = partId;
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
    this.storageType = storageType;
  }

  public PartSampleNeighborResult() {
    this(-1, null, NeighborStorageType.LONGARRAY);
  }

  public Long2ObjectOpenHashMap<Neighbor> getNodeIdToSampleNeighbors() {
    return nodeIdToSampleNeighbors;
  }

  public void setNodeIdToSampleNeighbors(Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors) {
    this.nodeIdToSampleNeighbors = nodeIdToSampleNeighbors;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, nodeIdToSampleNeighbors.size());
    ByteBufSerdeUtils.serializeByte(output, storageType.getIndex());

    if (storageType == NeighborStorageType.LONGARRAY) {
      for (Long2ObjectOpenHashMap.Entry<Neighbor> entry : nodeIdToSampleNeighbors
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        Neighbor sampleNeighbors = entry.getValue();
        if (sampleNeighbors == null) {
          sampleNeighbors = new LongNeighbor(new long[0]);
        }
        sampleNeighbors.serialize(output);
      }
    } else {
      for (Long2ObjectOpenHashMap.Entry<Neighbor> entry : nodeIdToSampleNeighbors
          .long2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
        Neighbor sampleNeighbors = entry.getValue();
        if (sampleNeighbors == null) {
          sampleNeighbors = new IntNeighbor(new int[0]);
        }
        sampleNeighbors.serialize(output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
    nodeIdToSampleNeighbors = new Long2ObjectOpenHashMap<>(size);

    if (storageType == NeighborStorageType.LONGARRAY) {
      for (int i = 0; i < size; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        LongNeighbor sampleNeighbors = new LongNeighbor();
        sampleNeighbors.deserialize(input);
        nodeIdToSampleNeighbors.put(nodeId, sampleNeighbors);
      }
    } else {
      for (int i = 0; i < size; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        IntNeighbor sampleNeighbors = new IntNeighbor();
        sampleNeighbors.deserialize(input);
        nodeIdToSampleNeighbors.put(nodeId, sampleNeighbors);
      }
    }

  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH + ByteBufSerdeUtils.BYTE_LENGTH;
    for (Long2ObjectOpenHashMap.Entry<Neighbor> entry : nodeIdToSampleNeighbors
        .long2ObjectEntrySet()) {
      len += ByteBufSerdeUtils.LONG_LENGTH;
      Neighbor sampleNeighbors = entry.getValue();
      len += sampleNeighbors.bufferLen();
    }
    return len;
  }
}