/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.matrix.psf.get.enhance.indexed;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Specified index get func result
 */
public class IndexGetResult extends PartitionGetResult {
  public int pid;
  public double[] values;

  /**
   * @param pid param id
   * @param values values of specified index array of one partition
   */
  public IndexGetResult(int pid, double[] values) {
    this.pid = pid;
    this.values = values;
  }

  public IndexGetResult() {
    this(-1, null);
  }

  public double[] getValues() {
    return values;
  }

  public int getParamId() {
    return pid;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(pid);
    buf.writeInt(values.length);
    for (int i = 0; i < values.length; i++)
      buf.writeDouble(values[i]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    this.pid = buf.readInt();
    int len = buf.readInt();
    values = new double[len];
    for (int i = 0; i < len; i++) {
      values[i] = buf.readDouble();
    }
  }

  @Override
  public int bufferLen() {
    return 4 + values.length * 8;
  }
}
