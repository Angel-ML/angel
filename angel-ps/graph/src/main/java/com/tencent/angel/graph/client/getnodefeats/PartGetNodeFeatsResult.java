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
package com.tencent.angel.graph.client.getnodefeats;

import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetNodeFeatsResult extends PartitionGetResult {

  private int partId;
  private IntFloatVector[] feats;

  public PartGetNodeFeatsResult(int partId, IntFloatVector[] feats) {
    this.partId = partId;
    this.feats = feats;
  }

  public PartGetNodeFeatsResult() {
    this(-1, null);
  }

  public int getPartId() {
    return partId;
  }

  public IntFloatVector[] getFeats() {
    return feats;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeInt(partId);
    output.writeInt(feats.length);
    for (int i = 0; i < feats.length; i++) {
      if (feats[i] == null) {
        output.writeBoolean(true);
      } else {
        output.writeBoolean(false);
        NodeUtils.serialize(feats[i], output);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = input.readInt();
    int len = input.readInt();
    feats = new IntFloatVector[len];
    for (int i = 0; i < len; i++) {
      boolean isNull = input.readBoolean();
      if(!isNull) {
        feats[i] = NodeUtils.deserialize(input);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 8;
    for (int i = 0; i < feats.length; i++) {
      if (feats[i] == null) {
        len += 4;
      } else {
        len += 4;
        len += NodeUtils.dataLen(feats[i]);
      }
    }
    return len;
  }
}
