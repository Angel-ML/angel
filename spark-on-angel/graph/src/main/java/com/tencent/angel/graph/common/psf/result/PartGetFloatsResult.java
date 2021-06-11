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
package com.tencent.angel.graph.common.psf.result;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class PartGetFloatsResult extends PartitionGetResult {

  private long[] nodeIds;
  private float[][] data;

  public PartGetFloatsResult(long[] nodeIds, float[][] data) {
    this.nodeIds = nodeIds;
    this.data = data;
  }

  public PartGetFloatsResult() {
    this(null, null);
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeLongs(output, nodeIds);
    ByteBufSerdeUtils.serialize2DFloats(output, data);
  }

  @Override
  public void deserialize(ByteBuf input) {
    nodeIds = ByteBufSerdeUtils.deserializeLongs(input);
    data = ByteBufSerdeUtils.deserialize2DFloats(input);
  }

  @Override
  public int bufferLen() {
    return ByteBufSerdeUtils.serializedLongsLen(nodeIds) + ByteBufSerdeUtils
        .serialized2DFloatsLen(data);
  }

  public long[] getNodeIds() {
    return nodeIds;
  }

  public void setNodeIds(long[] nodeIds) {
    this.nodeIds = nodeIds;
  }

  public float[][] getData() {
    return data;
  }

  public void setData(float[][] data) {
    this.data = data;
  }
}