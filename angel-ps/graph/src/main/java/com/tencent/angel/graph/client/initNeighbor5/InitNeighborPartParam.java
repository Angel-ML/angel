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
package com.tencent.angel.graph.client.initNeighbor5;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNeighborPartParam extends PartitionUpdateParam {

  private long[] keys;
  private int[] index;
  private int[] indptr;
  private long[] neighbors;
  private int[] edgeTypes;
  private int[] dstTypes;
  private int startIndex;
  private int endIndex;
  private long[][] neighborArrays;
  private int[][] edgeTypeArrays;
  private int[][] dstTypeArrays;

  public InitNeighborPartParam(int matrixId, PartitionKey pkey,
                               long[] keys, int[] index, int[] indptr,
                               long[] neighbors, int[] edgeTypes, int[] dstTypes,
                               int startIndex, int endIndex) {
    super(matrixId, pkey);
    this.keys = keys;
    this.index = index;
    this.indptr = indptr;
    this.neighbors = neighbors;
    this.edgeTypes = edgeTypes;
    this.dstTypes = dstTypes;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public InitNeighborPartParam() {
    this(0, null, null, null, null, null, null, null, 0, 0);
  }

  public long[] getKeys() {
    return keys;
  }

  public long[][] getNeighborArrays() {
    return neighborArrays;
  }

  public int[][] getEdgeTypeArrays() {
    return edgeTypeArrays;
  }

  public int[][] getDstTypeArrays() {
    return dstTypeArrays;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (edgeTypes != null)
      buf.writeBoolean(true);
    else
      buf.writeBoolean(false);

    if (dstTypes != null)
      buf.writeBoolean(true);
    else
      buf.writeBoolean(false);

    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      long key = keys[index[i]];
      int len = indptr[index[i] + 1] - indptr[index[i]];
      buf.writeLong(key);
      buf.writeInt(len);
      for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
        buf.writeLong(neighbors[j]);
      if (edgeTypes != null) {
        for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
          buf.writeInt(edgeTypes[j]);
      }
      if (dstTypes != null) {
        for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
          buf.writeInt(dstTypes[j]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    boolean hasEdgeType = buf.readBoolean();
    boolean hasDstType = buf.readBoolean();

    int size = buf.readInt();
    keys = new long[size];
    neighborArrays = new long[size][];

    if (hasEdgeType)
      edgeTypeArrays = new int[size][];

    if (hasDstType)
      dstTypeArrays = new int[size][];

    for (int i = 0; i < size; i++) {
      long node = buf.readLong();
      keys[i] = node;
      int len = buf.readInt();
      long[] neighbors = new long[len];
      for (int j = 0; j < len; j++)
        neighbors[j] = buf.readLong();
      neighborArrays[i] = neighbors;

      if (hasEdgeType) {
        int[] types = new int[len];
        for (int j = 0; j < len; j++)
          types[j] = buf.readInt();
        edgeTypeArrays[i] = types;
      }

      if (hasDstType) {
        int[] types = new int[len];
        for (int j = 0; j < len; j++)
          types[j] = buf.readInt();
        dstTypeArrays[i] = types;
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4 + ByteBufSerdeUtils.serializedBooleanLen(true) + ByteBufSerdeUtils.serializedBooleanLen(true);
    for (int i = startIndex; i < endIndex; i++) {
      len += 12;
      len += 8 * (indptr[index[i] + 1] - indptr[index[i]]);
      if (edgeTypes != null)
        len += 4 * (indptr[index[i] + 1] - indptr[index[i]]);
      if (dstTypes != null)
        len += 4 * (indptr[index[i] + 1] - indptr[index[i]]);
    }
    return len;
  }
}
