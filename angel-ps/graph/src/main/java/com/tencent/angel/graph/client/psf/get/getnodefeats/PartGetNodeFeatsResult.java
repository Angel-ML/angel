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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.data.*;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartGetNodeFeatsResult extends PartitionGetResult {

  private int partId;
  private Long2ObjectOpenHashMap<ByteArrayElement> nodeId2Feats;
  private FeatureKeyType keyType;

  public PartGetNodeFeatsResult(int partId, Long2ObjectOpenHashMap<ByteArrayElement> nodeIdTofeats, FeatureKeyType keyType) {
    this.partId = partId;
    this.nodeId2Feats = nodeIdTofeats;
    this.keyType = keyType;
  }

  public PartGetNodeFeatsResult() {
    this(-1, null, FeatureKeyType.INT);
  }

  public int getPartId() {
    return partId;
  }

  public FeatureKeyType getKeyType() {
    return keyType;
  }

  public Long2ObjectOpenHashMap<ByteArrayElement> getNodeIdToFeats() {
    return nodeId2Feats;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, nodeId2Feats.size());
    for (Long2ObjectOpenHashMap.Entry<ByteArrayElement> entry : nodeId2Feats.long2ObjectEntrySet()) {
      ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
      entry.getValue().serialize(output);
    }
    ByteBufSerdeUtils.serializeByte(output, keyType.getIndex());
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    nodeId2Feats = new Long2ObjectOpenHashMap<>(size);
    try {
      for (int i = 0; i < size; i++) {
        long nodeId = ByteBufSerdeUtils.deserializeLong(input);
        ByteArrayElement nodeFeats = new ByteArrayElement();
        nodeFeats.deserialize(input);

        nodeId2Feats.put(nodeId, nodeFeats);
      }
      keyType = FeatureKeyType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
    } catch (Exception e) {
      throw new AngelException("Failed to deserialize node features for node id: " + e.getMessage(), e);
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (Long2ObjectOpenHashMap.Entry<ByteArrayElement> entry : nodeId2Feats.long2ObjectEntrySet()) {
      len += ByteBufSerdeUtils.LONG_LENGTH;
      len += entry.getValue().bufferLen();
    }
    len += ByteBufSerdeUtils.BYTE_LENGTH;
    return len;
  }
}