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

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

public class LongIndexGetResult extends PartitionGetResult {
  public PartitionKey partKey;
  public double[] values;

  /**
   * @param partKey partition key
   * @param values values of specified index array of one partition
   */
  public LongIndexGetResult(PartitionKey partKey, double[] values) {
    this.partKey = partKey;
    this.values = values;
  }

  public LongIndexGetResult() {
    this(null,null);
  }

  public double[] getValues() {
    return values;
  }

  @Override
  public void serialize(ByteBuf buf) {
    partKey.serialize(buf);
    buf.writeInt(values.length);
    for (int i = 0; i < values.length; i++)
      buf.writeDouble(values[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partKey = new PartitionKey();
    partKey.deserialize(buf);
    int len = buf.readInt();
    values = new double[len];
    for (int i = 0; i < len; i++) {
      values[i] = buf.readDouble();
    }
  }

  @Override
  public int bufferLen() {
    return partKey.bufferLen() + values.length * 8;
  }

  public PartitionKey getPartKey() {
    return partKey;
  }
}
