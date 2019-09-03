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
package com.tencent.angel.graph.client.initNeighbor4;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartInitNeighborParam extends PartitionUpdateParam {

  private Long2ObjectMap<long[]> nodesToNeighbors;
  private long[] keys;
  private int[] index;
  private int[] indptr;
  private long[] neighbors;
  private int startIndex;
  private int endIndex;

  public PartInitNeighborParam(int matrixId, PartitionKey pkey,
                               long[] keys, int[] index, int[] indptr,
                               long[] neighbors, int startIndex,
                               int endIndex) {
    super(matrixId, pkey);
    this.keys = keys;
    this.index = index;
    this.indptr = indptr;
    this.neighbors = neighbors;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitNeighborParam() {
    this(0, null, null, null, null, null, 0, 0);
  }

  public Long2ObjectMap<long[]> getNodesToNeighbors() {
    return nodesToNeighbors;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      long key = keys[index[i]];
      int len = indptr[index[i] + 1] - indptr[index[i]];
      buf.writeLong(key);
      buf.writeInt(len);
      for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
        buf.writeLong(neighbors[j]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = buf.readInt();
    nodesToNeighbors = new Long2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      long node = buf.readLong();
      int len = buf.readInt();
      long[] neighbors = new long[len];
      for (int j = 0; j < len; j++)
        neighbors[j] = buf.readLong();
      nodesToNeighbors.put(node, neighbors);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      len += 12;
      len += 8 * (indptr[index[i] + 1] - indptr[index[i]]);
    }
    return len;
  }
}
