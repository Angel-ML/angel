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
package com.tencent.angel.graph.client.initnodefeats4;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitNodeFeatsPartParam extends PartitionUpdateParam {
  private long[] keys;
  private IntFloatVector[] feats;
  private int[] index;
  private int startIndex;
  private int endIndex;

  public InitNodeFeatsPartParam(int matrixId, PartitionKey partKey,
                                long[] keys, IntFloatVector[] feats,
                                int[] index, int startIndex, int endIndex) {
    super(matrixId, partKey);
    this.keys = keys;
    this.feats = feats;
    this.index = index;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public InitNodeFeatsPartParam() {
    this(-1, null, null, null, null, -1, -1);
  }

  public long[] getNodeIds() {
    return keys;
  }

  public IntFloatVector[] getFeats() {
    return feats;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    int writeIndex = buf.writerIndex();
    int writeNum = 0;
    buf.writeInt(0);
    for (int i = startIndex; i < endIndex; i++) {
      if (feats[index[i]] == null || feats[index[i]].getSize() == 0)
        continue;
      buf.writeLong(keys[index[i]]);
      NodeUtils.serialize(feats[index[i]], buf);
      writeNum++;
    }
    buf.setInt(writeIndex, writeNum);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    keys = new long[len];
    feats = new IntFloatVector[len];

    for (int i = 0; i < len; i++) {
      keys[i] = buf.readLong();
      feats[i] = NodeUtils.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      if (feats[index[i]] != null && feats[index[i]].getSize() != 0) {
        len += 8;
        len += NodeUtils.dataLen(feats[i]);
      }
    }
    return len;
  }
}
