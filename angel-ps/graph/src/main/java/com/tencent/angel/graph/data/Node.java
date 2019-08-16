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

package com.tencent.angel.graph.data;

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

  public Node(IntFloatVector feats, long[] neighbors) {
    this.feats = feats;
    this.neighbors = neighbors;
  }

  public Node() {
    this(null, null);
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

  @Override
  public Node deepClone() {
    IntFloatVector cloneFeats = feats.clone();

    long[] cloneNeighbors = new long[neighbors.length];
    System.arraycopy(neighbors, 0, cloneNeighbors, 0, neighbors.length);
    return new Node(cloneFeats, cloneNeighbors);
  }

  @Override
  public void serialize(ByteBuf output) {
    NodeUtils.serialize(feats, output);

    output.writeInt(neighbors.length);
    for (int i = 0; i < neighbors.length; i++) {
      output.writeLong(neighbors[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    feats = NodeUtils.deserialize(input);

    int len = input.readInt();
    neighbors = new long[len];
    for (int i = 0; i < len; i++) {
      neighbors[i] = input.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return NodeUtils.dataLen(feats) + 4 + 8 * neighbors.length;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    NodeUtils.serialize(feats, output);

    output.writeInt(neighbors.length);
    for (int i = 0; i < neighbors.length; i++) {
      output.writeLong(neighbors[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    feats = NodeUtils.deserialize(input);

    int len = input.readInt();
    neighbors = new long[len];
    for (int i = 0; i < len; i++) {
      neighbors[i] = input.readLong();
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
