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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.params.PartitionUpdateParamWithIds;
import com.tencent.angel.graph.client.node2vec.utils.SerDe;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PushNeighborPartitionParam extends PartitionUpdateParamWithIds {
  private Long2ObjectOpenHashMap<long[]> nodeIdToNeighborIndices;

  public PushNeighborPartitionParam(int matrixId, PartitionKey partKey,
                                    Long2ObjectOpenHashMap<long[]> nodeIdToNeighborIndices,
                                    long[] nodeIds, int startIndex, int endIndex) {
    super(matrixId, partKey, nodeIds, startIndex, endIndex);
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
  }

  public PushNeighborPartitionParam() {

  }

  public Long2ObjectOpenHashMap<long[]> getNodeIdToNeighborIndices() {
    return nodeIdToNeighborIndices;
  }

  @Override
  protected void clear() {
    super.clear();
    nodeIdToNeighborIndices = null;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    SerDe.serLong2ArrayHashMap(keyIds, startIdx, endIdx, nodeIdToNeighborIndices, buf);
    clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    nodeIdToNeighborIndices = SerDe.deserLong2LongArray(buf);
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += SerDe.getLong2ArrayHashMapSerSize(keyIds, startIdx, endIdx, nodeIdToNeighborIndices);
    return len;
  }
}
