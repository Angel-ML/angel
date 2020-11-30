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
package com.tencent.angel.graph.client.node2vec.data;

import com.tencent.angel.graph.client.node2vec.PartitionHasher;
import com.tencent.angel.graph.client.node2vec.utils.SerDe;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class WalkPath implements IElement {
  private long[] path;

  public int getCurrPathIdx() {
    return currPathIdx;
  }

  public void setCurrPathIdx(int currPathIdx) {
    this.currPathIdx = currPathIdx;
  }

  private int currPathIdx = 0;
  private int mod;
  private int nextPartitionIdx = -1;

  public WalkPath() {
  }

  public WalkPath(long[] path, int mod) {
    this.path = path;
    this.currPathIdx = path.length;
    this.mod = mod;
    updatePartitionIdx();
  }

  public WalkPath(long[] path, int mod, int currPathIdx) {
    this.path = path;
    this.currPathIdx = currPathIdx;
    this.mod = mod;
    updatePartitionIdx();
  }

  public WalkPath(int pathLength, int mod, long... eles) {
    path = new long[pathLength];
    if (eles != null) {
      for (long e : eles) {
        path[currPathIdx] = e;
        currPathIdx++;
      }
    }
    this.mod = mod;
    updatePartitionIdx();
  }

  public int getNextPartitionIdx() {
    return nextPartitionIdx;
  }

  public long[] getPath() {
    return path;
  }

  public void setPath(long[] path) {
    this.path = path;
  }

  public int getMod() {
    return mod;
  }

  public void setMod(int mod) {
    this.mod = mod;
  }

  public WalkPath add2Path(long ele) {
    assert currPathIdx < path.length;

    path[currPathIdx] = ele;
    currPathIdx++;
    updatePartitionIdx();
    return this;
  }

  public long[] getTail2() {
    return new long[]{path[currPathIdx - 2], path[currPathIdx - 1]};
  }

  public long getHead() { return path[0]; }

  private void updatePartitionIdx() {
    nextPartitionIdx = PartitionHasher.getHash(path[currPathIdx - 2], path[currPathIdx - 1], mod);
  }

  public boolean isComplete() {
    return currPathIdx >= path.length;
  }

  @Override
  public Object deepClone() {
    long[] clonedPath = path.clone();
    return new WalkPath(clonedPath, this.mod, this.currPathIdx);
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(mod);
    output.writeInt(currPathIdx);
    if (path == null) {
      output.writeInt(0);
    } else {
      SerDe.serArray(path, output);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    mod = input.readInt();
    currPathIdx = input.readInt();
    path = SerDe.deserLongArray(input);
    updatePartitionIdx();
  }

  @Override
  public int bufferLen() {
    int len = 8;
    if (path != null) {
      len += currPathIdx * 8;
    }
    return len;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    output.writeInt(mod);
    output.writeInt(currPathIdx);
    if (path == null) {
      output.writeInt(0);
    } else {
      output.writeInt(path.length);

      for (long e : path) {
        output.writeLong(e);
      }
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    mod = input.readInt();
    currPathIdx = input.readInt();

    int len = input.readInt();
    if (len > 0) {
      path = new long[len];
      for (int i = 0; i < len; i++) {
        path[i] = input.readLong();
      }
    }

    updatePartitionIdx();
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}
