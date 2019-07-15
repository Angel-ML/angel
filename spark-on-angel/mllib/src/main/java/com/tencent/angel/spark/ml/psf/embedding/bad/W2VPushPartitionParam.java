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
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class W2VPushPartitionParam extends PartitionUpdateParam {
  int[] indices;
  float[] deltas;
  int numNodePerRow;
  int dimension;
  int start;
  int length;
  ByteBuf buf;

  public W2VPushPartitionParam(int matrixId,
                               PartitionKey pkey,
                               int[] indices,
                               float[] deltas,
                               int numNodePerRow,
                               int start,
                               int length,
                               int dimension) {
    super(matrixId, pkey);
    this.indices = indices;
    this.numNodePerRow = numNodePerRow;
    this.start = start;
    this.dimension = dimension;
    this.length = length;
    this.deltas = deltas;
  }

  public W2VPushPartitionParam() { }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(numNodePerRow);
    buf.writeInt(dimension);
    buf.writeInt(length);
    for (int i = 0; i < length; i++) {
      buf.writeInt(indices[i + start]);
      int offset = (start + i) * dimension * 2;
      for (int j = 0; j < dimension * 2; j++)
        buf.writeFloat(deltas[offset + j]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.numNodePerRow = buf.readInt();
    this.dimension = buf.readInt();
    this.length = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();
  }

  public void clear() {
    buf.release();
  }


  @Override
  public int bufferLen() {
    return super.bufferLen();
  }
}
