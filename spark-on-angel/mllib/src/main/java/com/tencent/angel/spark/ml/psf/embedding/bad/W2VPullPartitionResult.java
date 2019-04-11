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

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class W2VPullPartitionResult extends PartitionGetResult {

  int start;
  int length;
  int dimension;
  float[] layers;
  ByteBuf buf;

  public W2VPullPartitionResult(int start, int dimension, float[] layers) {
    this.start = start;
    this.layers = layers;
    this.dimension = dimension;
  }

  public W2VPullPartitionResult() {}

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(start);
    buf.writeInt(dimension);
    buf.writeInt(layers.length);
    for (int a = 0; a < layers.length; a ++) buf.writeFloat(layers[a]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    start = buf.readInt();
    dimension = buf.readInt();
    length = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();
  }

  public void merge(float[] results) {
    int offset = start * dimension * 2;
    for (int a = 0; a < length; a ++)
      results[a + offset] = buf.readFloat();
  }

  public void clear() {
    this.buf.release();
  }

  @Override
  public int bufferLen() {
    return 4 + 4 + layers.length * 4;
  }
}
