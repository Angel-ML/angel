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
package com.tencent.angel.graph.client.psf.get.getneighbors;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartGetNeighborsResult extends PartitionGetResult {

  private int partId;
  private Long2ObjectOpenHashMap<long[]> nodeIdToContents;

  public PartGetNeighborsResult(int partId, Long2ObjectOpenHashMap<long[]> nodeIdToContents) {
    this.partId = partId;
    this.nodeIdToContents = nodeIdToContents;
  }

  public PartGetNeighborsResult() {
    this(-1, null);
  }

  public int getPartId() {
    return partId;
  }

  public Long2ObjectOpenHashMap<long[]> getNodeIdToContents() {
    return nodeIdToContents;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, nodeIdToContents.size());
    for (Long2ObjectOpenHashMap.Entry<long[]> entry : nodeIdToContents.long2ObjectEntrySet()) {
      ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
      ByteBufSerdeUtils.serializeLongs(output, entry.getValue());
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    nodeIdToContents = new Long2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      nodeIdToContents.put(ByteBufSerdeUtils.deserializeLong(input),
          ByteBufSerdeUtils.deserializeLongs(input));
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (Long2ObjectOpenHashMap.Entry<long[]> entry : nodeIdToContents.long2ObjectEntrySet()) {
      len += ByteBufSerdeUtils.LONG_LENGTH;
      len += ByteBufSerdeUtils.serializedLongsLen(entry.getValue());
    }
    return len;
  }
}