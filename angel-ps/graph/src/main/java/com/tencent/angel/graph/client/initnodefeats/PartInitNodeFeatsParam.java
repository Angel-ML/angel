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
package com.tencent.angel.graph.client.initnodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class PartInitNodeFeatsParam extends PartitionUpdateParam {

  private int[] nodeIds;
  private IntFloatVector[] feats;
  int startIndex;
  int endIndex;

  public PartInitNodeFeatsParam(int matrixId, PartitionKey partKey,
      int[] nodeIds, IntFloatVector[] feats, int startIndex, int endIndex) {
    super(matrixId, partKey);
    this.nodeIds = nodeIds;
    this.feats = feats;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitNodeFeatsParam() {
    this(-1, null, null, null, -1, -1);
  }

  public int[] getNodeIds() {
    return nodeIds;
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
      if (feats[i] == null || feats[i].getSize() == 0) {
        continue;
      }
      buf.writeInt(nodeIds[i]);
      NodeUtils.serialize(feats[i], buf);
      writeNum++;
    }
    buf.setInt(writeIndex, writeNum);
  }


  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    nodeIds = new int[len];
    feats = new IntFloatVector[len];

    for (int i = 0; i < len; i++) {
      nodeIds[i] = buf.readInt();
      feats[i] = NodeUtils.deserialize(buf);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      if (feats[i] != null && feats[i].getSize() != 0) {
        len += 4;
        len += NodeUtils.dataLen(feats[i]);
      }
    }
    return len;
  }
}
