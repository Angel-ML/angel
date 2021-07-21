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
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartGetNodeFeatsResult extends PartitionGetResult {

  private int partId;
  private Long2ObjectOpenHashMap<IntFloatVector> nodeIdTofeats;

  public PartGetNodeFeatsResult(int partId, Long2ObjectOpenHashMap<IntFloatVector> nodeIdTofeats) {
    this.partId = partId;
    this.nodeIdTofeats = nodeIdTofeats;
  }

  public PartGetNodeFeatsResult() {
    this(-1, null);
  }

  public int getPartId() {
    return partId;
  }

  public Long2ObjectOpenHashMap<IntFloatVector> getNodeIdTofeats() {
    return nodeIdTofeats;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, nodeIdTofeats.size());
    for (Long2ObjectOpenHashMap.Entry<IntFloatVector> entry : nodeIdTofeats.long2ObjectEntrySet()) {
      ByteBufSerdeUtils.serializeLong(output, entry.getLongKey());
      ByteBufSerdeUtils.serializeVector(output, entry.getValue());
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    nodeIdTofeats = new Long2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      long nodeId = ByteBufSerdeUtils.deserializeLong(input);
      IntFloatVector nodeFeats = (IntFloatVector) ByteBufSerdeUtils.deserializeVector(input);
      nodeIdTofeats.put(nodeId, nodeFeats);
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (Long2ObjectOpenHashMap.Entry<IntFloatVector> entry : nodeIdTofeats.long2ObjectEntrySet()) {
      len += ByteBufSerdeUtils.LONG_LENGTH;
      len += ByteBufSerdeUtils.serializedVectorLen(entry.getValue());
    }
    return len;
  }
}