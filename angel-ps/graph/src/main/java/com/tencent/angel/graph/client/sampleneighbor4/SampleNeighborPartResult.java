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
package com.tencent.angel.graph.client.sampleneighbor4;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import io.netty.buffer.ByteBuf;
import java.util.Random;

public class SampleNeighborPartResult extends PartitionGetResult {

  private int partId;
  private int[] indptr;
  private int[] types;
  private long[] neighbors;

  ServerLongAnyRow row;
  long[] keys;
  int numSample;
  boolean sampleTypes;

  public SampleNeighborPartResult(int partId, ServerLongAnyRow row,
      long[] keys, int numSample,
      boolean sampleTypes) {
    this.partId = partId;
    this.keys = keys;
    this.numSample = numSample;
    this.row = row;
    this.sampleTypes = sampleTypes;
  }

  public SampleNeighborPartResult() {

  }

  public int[] getIndptr() {
    return indptr;
  }

  public long[] getNeighbors() {
    return neighbors;
  }

  public int[] getTypes() {
    return types;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf buf) {
    // sample happens here to avoid memory copy on servers
    Random rand = new Random(System.currentTimeMillis());

    buf.writeInt(partId); // write partition id first
    buf.writeBoolean(sampleTypes);
    buf.writeInt(keys.length);
    int writeIndex = buf.writerIndex();
    buf.writeInt(0);

    int length = 0;
    for (int i = 0; i < keys.length; i++) {
      Node node = (Node) row.get(keys[i]);
      if (node == null) {
        buf.writeInt(0); // size for this node
        continue;
      }

      long[] neighbor = node.getNeighbors();
      int[] types = node.getTypes();
      if (neighbor == null || neighbor.length == 0) {
        buf.writeInt(0);
        continue;
      }

      int size = numSample;
      if (numSample <= 0 || numSample >= neighbor.length) {
        size = neighbor.length;
      }

      length += size; // # neighbors/types
      buf.writeInt(size);

      int start = rand.nextInt(neighbor.length);
      if (sampleTypes) {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          buf.writeLong(neighbor[idx]);
          buf.writeInt(types[idx]);
        }
      } else {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          buf.writeLong(neighbor[idx]);
        }
      }
    }

    buf.setInt(writeIndex, length);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partId = buf.readInt();
    sampleTypes = buf.readBoolean();
    int keysLen = buf.readInt();
    int neighborLen = buf.readInt();

    indptr = new int[keysLen + 1];
    neighbors = new long[neighborLen];
    if (sampleTypes) {
      types = new int[neighborLen];
    }

    int idx1 = 0, idx2 = 0;
    indptr[idx1++] = 0;
    if (!sampleTypes) {
      for (int i = 0; i < keysLen; i++) {
        int size = buf.readInt();
        for (int j = 0; j < size; j++) {
          neighbors[idx2++] = buf.readLong();
        }
        indptr[idx1++] = idx2;
      }
    } else {
      for (int i = 0; i < keysLen; i++) {
        int size = buf.readInt();
        for (int j = 0; j < size; j++) {
          neighbors[idx2] = buf.readLong();
          types[idx2++] = buf.readInt();
        }
        indptr[idx1++] = idx2;
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 4 + 4 + 4 + ByteBufSerdeUtils.serializedBooleanLen(sampleTypes);

    for (int i = 0; i < keys.length; i++) {
      Node node = (Node) row.get(keys[i]);
      if (node == null) {
        len += 4;
        continue;
      }

      long[] neighbor = node.getNeighbors();
      if (neighbor == null || neighbor.length == 0) {
        len += 4;
        continue;
      }

      int size = numSample;
      if (numSample <= 0 || numSample >= neighbor.length) {
        size = neighbor.length;
      }

      len += 4;

      if (sampleTypes) {
        for (int j = 0; j < size; j++) {
          len += 8 + 4;
        }
      } else {
        for (int j = 0; j < size; j++) {
          len += 8;
        }
      }
    }
    return len;
  }


}
