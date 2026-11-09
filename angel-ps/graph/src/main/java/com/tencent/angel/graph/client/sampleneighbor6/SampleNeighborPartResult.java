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
package com.tencent.angel.graph.client.sampleneighbor6;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Random;
import scala.collection.Iterator;

public class SampleNeighborPartResult extends PartitionGetResult {
  private int partId;
  private int[] indptr;
  private int[] types;
  private long[] neighbors;

  ServerLongAnyRow row;
  long[] keys;
  long[] withoutNeighKeys;//not contain some edges(neighbors), same len as keys
  long[] containNeighKeys;//must contain some neighbors
  int numSample;
  boolean sampleTypes;

  public SampleNeighborPartResult(int partId, ServerLongAnyRow row,
      long[] keys, long[] withoutNeighKeys, long[] containNeighKeys, int numSample,
      boolean sampleTypes) {
    this.partId = partId;
    this.keys = keys;
    this.withoutNeighKeys = withoutNeighKeys;
    this.containNeighKeys = containNeighKeys;
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

    buf.writeInt(partId); // write partition id first
    buf.writeBoolean(sampleTypes);
    buf.writeInt(keys.length);
    boolean withoutKey = withoutNeighKeys != null;
    boolean containKey = containNeighKeys != null;
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
      int[] types = node.getEdgeTypes();
      if (neighbor == null || neighbor.length == 0) {
        buf.writeInt(0);
        continue;
      }

      int size = numSample;
      if (numSample <= 0 || numSample >= neighbor.length)
        size = neighbor.length;

      ArrayList<Long> neighs = new ArrayList<>();
      ArrayList<Integer> nTypes = new ArrayList<>();
      int start = 0;
      if (sampleTypes) {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          if (withoutKey) {
            if (neighbor[idx] == withoutNeighKeys[i]) {
              continue;
            } else {
              if (types[idx] != -1) {
                neighs.add(neighbor[idx]);
                nTypes.add(types[idx]);
              }
            }
          }
          if (containKey) {
            for (int k = 0; k < containNeighKeys.length; k++) {
              if (containNeighKeys[k] == neighbor[idx]) {
                if (types[idx] != -1) {
                  neighs.add(neighbor[idx]);
                  nTypes.add(types[idx]);
                }
              }
            }
          }
        }
        size = neighs.size();
        buf.writeInt(size);
        for (int j = 0; j < size; j++) {
          buf.writeLong(neighs.get(j));
          buf.writeInt(nTypes.get(j));
        }

      } else {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          if (withoutKey) {
            if (neighbor[idx] == withoutNeighKeys[i]) {
              continue;
            } else {
              neighs.add(neighbor[idx]);
            }
          }
          if (containKey) {
            for (int k = 0; k < containNeighKeys.length; k++) {
              if (containNeighKeys[k] == neighbor[idx]) {
                neighs.add(neighbor[idx]);
              }
            }
          }
        }
        size = neighs.size();
        buf.writeInt(size);
        for (int j = 0; j < size; j++) {
          buf.writeLong(neighs.get(j));
        }
      }
      length += size;
    }

    buf.setInt(writeIndex, length);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partId = buf.readInt();
    sampleTypes = buf.readBoolean();
    int keysLen = buf.readInt();
    int neighborLen = buf.readInt();
    neighbors = new long[neighborLen];
    types = new int[neighborLen];


    for (int i = 0; i < keysLen; i++) {
      int size = buf.readInt();
      if (size != 0) {
        indptr = new int[keysLen + 1];

        int idx1 = 0, idx2 = 0;
        indptr[idx1++] = 0;
        if (!sampleTypes) {
          for (int j = 0; j < size; j++) {
            neighbors[idx2++] = buf.readLong();
          }
          indptr[idx1++] = idx2;
        } else {
          for (int j = 0; j < size; j++) {
            neighbors[idx2] = buf.readLong();
            types[idx2++] = buf.readInt();
          }
          indptr[idx1++] = idx2;
        }
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
        len +=4;
        continue;
      }

      int size = numSample;
      if (numSample <= 0 || numSample >= neighbor.length)
        size = neighbor.length;

      len += 4;
      int start = 0;
      if (sampleTypes) {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          if (withoutNeighKeys != null) {
            if (neighbor[idx] == withoutNeighKeys[i]) {
              continue;
            } else {
              len += 8 + 4;
            }
          }
          if (containNeighKeys != null) {
            for (int k = 0; k < containNeighKeys.length; k++) {
              if (containNeighKeys[k] == neighbor[idx]) {
                len += 8 + 4;
              }
            }
          }
        }
      } else {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          if (withoutNeighKeys != null) {
            if (neighbor[idx] == withoutNeighKeys[i]) {
              continue;
            } else {
              len += 8;
            }
          }
          if (containNeighKeys != null) {
            for (int k = 0; k < containNeighKeys.length; k++) {
              if (containNeighKeys[k] == neighbor[idx]) {
                len += 8;
              }
            }
          }
        }
      }
    }
    return len;
  }


}