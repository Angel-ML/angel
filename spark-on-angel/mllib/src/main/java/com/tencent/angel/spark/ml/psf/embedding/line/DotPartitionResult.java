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

package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class DotPartitionResult extends PartitionGetResult {
  float[] values;
  int length;
  ByteBuf buf;

  public DotPartitionResult(float[] values) {
    this.values = values;
  }

  public DotPartitionResult() {
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(values.length);
    for (float value : values) {
      buf.writeFloat(value);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.length = buf.readInt();
    this.buf = buf.duplicate();
    this.buf.retain();
  }

  @Override
  public int bufferLen() {
    return 4 + values.length * 4;
  }

  public void merge(float[] result) {
    for (int c = 0; c < result.length; c++) result[c] += buf.readFloat();
  }

  public void clear() {
    buf.release();
  }
}
