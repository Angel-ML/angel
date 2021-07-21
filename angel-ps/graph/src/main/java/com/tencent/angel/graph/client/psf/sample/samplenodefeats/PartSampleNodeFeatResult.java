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

package com.tencent.angel.graph.client.psf.sample.samplenodefeats;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartSampleNodeFeatResult extends PartitionGetResult {

  /**
   * Partition id
   */
  public int partId;

  /**
   * Node feats
   */
  public IntFloatVector[] feats;

  public PartSampleNodeFeatResult(int partId, IntFloatVector[] feats) {
    this.partId = partId;
    this.feats = feats;
  }

  public PartSampleNodeFeatResult() {
    this(-1, null);
  }

  public IntFloatVector[] getNodeFeats() {
    return feats;
  }

  public void setNodeFeats(IntFloatVector[] feats) {
    this.feats = feats;
  }

  public int getPartId() {
    return partId;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeInt(output, partId);
    ByteBufSerdeUtils.serializeInt(output, feats.length);
    for (IntFloatVector feat : feats) {
      if (feat == null) {
        ByteBufSerdeUtils.serializeBoolean(output, true);
      } else {
        ByteBufSerdeUtils.serializeBoolean(output, false);
        ByteBufSerdeUtils.serializeVector(output, feat);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    partId = ByteBufSerdeUtils.deserializeInt(input);
    int size = ByteBufSerdeUtils.deserializeInt(input);
    feats = new IntFloatVector[size];
    for (int i = 0; i < size; i++) {
      boolean isNull = ByteBufSerdeUtils.deserializeBoolean(input);
      if (!isNull) {
        feats[i] = (IntFloatVector) ByteBufSerdeUtils.deserializeVector(input);
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 2 * ByteBufSerdeUtils.INT_LENGTH;
    for (IntFloatVector feat : feats) {
      len += ByteBufSerdeUtils.BOOLEN_LENGTH;
      if (feat != null) {
        len += ByteBufSerdeUtils.serializedVectorLen(feat);
      }
    }
    return len;
  }
}
