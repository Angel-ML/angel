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
package com.tencent.angel.spark.ml.psf.gcn;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class GetLabelsPartResult extends PartitionGetResult {
  private long[] keys;
  private float[] values;

  public GetLabelsPartResult(long[] keys, float[] values) {
    this.keys = keys;
    this.values = values;
  }

  public GetLabelsPartResult() {}

  public long[] getKeys() {
    return keys;
  }

  public float[] getValues() {
    return values;
  }

  public int size() {
    return keys.length;
  }

  @Override
  public void serialize(ByteBuf output) {
    assert (keys.length == values.length);
    output.writeInt(keys.length);
    for (int i = 0; i < keys.length; i++)
      output.writeLong(keys[i]);
    for (int i = 0; i < values.length; i++)
      output.writeFloat(values[i]);
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    keys = new long[len];
    values = new float[len];
    for (int i = 0; i < len; i++)
      keys[i] = input.readLong();
    for (int i = 0; i < len; i++)
      values[i] = input.readFloat();
  }

  @Override
  public int bufferLen() {
    int len = 4;
    len += 4 * keys.length + 8 * values.length;
    return len;
  }

}
