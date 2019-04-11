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

package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class W2VPullParatitionParam extends PartitionGetParam {

  int[] indices;
  int numNodePerRow;
  int dimension;
  int start;
  int length;

  public W2VPullParatitionParam(int matrixId,
                                PartitionKey partKey,
                                int[] indices,
                                int numNodePerRow,
                                int start,
                                int length,
                                int dimension) {
    super(matrixId, partKey);
    this.indices = indices;
    this.numNodePerRow = numNodePerRow;
    this.start = start;
    this.dimension = dimension;
    this.length = length;
  }

  public W2VPullParatitionParam() { }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    numNodePerRow = buf.readInt();
    start = buf.readInt();
    dimension = buf.readInt();
    length = buf.readInt();
    indices = new int[length];
    for (int i = 0; i < length; i ++) indices[i] = buf.readInt();
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(numNodePerRow);
    buf.writeInt(start);
    buf.writeInt(dimension);
    buf.writeInt(length);
    for (int i = 0; i < length; i ++) buf.writeInt(indices[i + start]);
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 4 + 4 * indices.length;
  }
}
