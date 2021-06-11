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

package com.tencent.angel.graph.common.data;

import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Node features
 */
public class Node implements IElement {

  private IntFloatVector feats;

  private long[] neighbors;
  private IntFloatVector[] edgeFeatures;
  private int[] types;
  private int[] edgeTypes;
  private float[] labels;

  public Node(IntFloatVector feats, long[] neighbors) {
    this(feats, neighbors, null);
  }

  public Node(IntFloatVector feats, long[] neighbors, int[] types) {
    this.feats = feats;
    this.neighbors = neighbors;
    this.types = types;
  }

  public Node(IntFloatVector feats, long[] neighbors, int[] types, float[] labels) {
    this(feats, neighbors, types);
    this.labels = labels;
  }

  public Node(IntFloatVector feats, long[] neighbors, IntFloatVector[] edgeFeatures, int[] types) {
    this(feats, neighbors, types);
    this.edgeFeatures = edgeFeatures;
  }

  public Node() {
    this(null, null, null);
  }

  public IntFloatVector getFeats() {
    return feats;
  }

  public void setFeats(IntFloatVector feats) {
    this.feats = feats;
  }

  public long[] getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(long[] neighbors) {
    this.neighbors = neighbors;
  }

  public IntFloatVector[] getEdgeFeatures() {
    return edgeFeatures;
  }

  public void setEdgeFeatures(IntFloatVector[] edgeFeats) {
    this.edgeFeatures = edgeFeats;
  }

  public int[] getTypes() {
    return types;
  }

  public void setTypes(int[] types) {
    this.types = types;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(int[] edgeTypes) {
    this.edgeTypes = edgeTypes;
  }

  public float[] getLabels() { return labels; }

  public void setLabels(float[] labels) { this.labels = labels; }

  @Override
  public Node deepClone() {
    if (feats != null && neighbors != null) {
      IntFloatVector cloneFeats = feats.clone();

      long[] cloneNeighbors = new long[neighbors.length];
      System.arraycopy(neighbors, 0, cloneNeighbors, 0, neighbors.length);

      if (types == null)
        return new Node(cloneFeats, cloneNeighbors);
      else {
        int[] cloneTypes = new int[types.length];
        System.arraycopy(types, 0, cloneTypes, 0, types.length);
        return new Node(cloneFeats, cloneNeighbors, cloneTypes);
      }
    } else {
      return new Node();
    }
  }

  @Override
  public void serialize(ByteBuf output) {
    if (feats !=null && neighbors != null ) {
      output.writeInt(1);
      NodeUtils.serialize(feats, output);

      output.writeInt(neighbors.length);
      for (int i = 0; i < neighbors.length; i++)
        output.writeLong(neighbors[i]);

      if (types != null) {
        output.writeInt(types.length);
        for (int i = 0; i < types.length; i++)
          output.writeInt(types[i]);
      } else {
        output.writeInt(0);
      }
    } else {
      output.writeInt(0);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int derFlag = input.readInt();
    if (0 != derFlag) {
      feats = NodeUtils.deserialize(input);

      int len = input.readInt();
      neighbors = new long[len];
      for (int i = 0; i < len; i++)
        neighbors[i] = input.readLong();

      len = input.readInt();
      if (0 != len) {
        types = new int[len];
        for (int i = 0; i < len; i++)
          types[i] = input.readInt();
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 4;
    if (feats != null && neighbors != null) {
      len = NodeUtils.dataLen(feats);
      len += 4 + 8 * neighbors.length;
      if (types != null) {
        len += 4 + 4 * types.length;
      } else {
        len += 4;
      }
    }
    return len;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    if (feats !=null && neighbors != null ) {
      output.writeInt(1);
      NodeUtils.serialize(feats, output);

      output.writeInt(neighbors.length);
      for (int i = 0; i < neighbors.length; i++)
        output.writeLong(neighbors[i]);

      if (types != null) {
        output.writeInt(types.length);
        for (int i = 0; i < types.length; i++)
          output.writeInt(types[i]);
      } else {
        output.writeInt(0);
      }
    } else {
      output.writeInt(0);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    int derFlag = input.readInt();
    if (0 != derFlag) {
      feats = NodeUtils.deserialize(input);

      int len = input.readInt();
      neighbors = new long[len];
      for (int i = 0; i < len; i++)
        neighbors[i] = input.readLong();

      len = input.readInt();
      if (0 != len) {
        types = new int[len];
        for (int i = 0; i < len; i++)
          types[i] = input.readInt();
      }
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
