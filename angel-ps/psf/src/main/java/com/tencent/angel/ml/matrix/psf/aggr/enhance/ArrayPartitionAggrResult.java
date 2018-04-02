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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;

import io.netty.buffer.ByteBuf;

/**
 * `ArrayPartitionAggrResult` is a result Class for all Aggregate function whose result is double[]
 */
public class ArrayPartitionAggrResult extends PartitionGetResult {
  private long[] cols;
  private double[] result;

  public ArrayPartitionAggrResult(long[] cols, double[] result) {
    this.cols = cols;
    this.result = result;
  }


  public ArrayPartitionAggrResult(double[] result) {
    this.result = result;
    this.cols = new long[]{};
  }

  public ArrayPartitionAggrResult() {}

  public long[] getCols() {
    return cols;
  }

  public double[] getResult() {
    return result;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(result.length);
    for (int i = 0; i < result.length; i++) {
      buf.writeDouble(result[i]);
    }

    buf.writeInt(cols.length);
    for (int i = 0; i < result.length; i++) {
      buf.writeLong(cols[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int resultSize = buf.readInt();
    result = new double[resultSize];
    for (int i = 0; i < resultSize; i++) {
      result[i] = buf.readDouble();
    }

    int colSize = buf.readInt();
    cols = new long[colSize];
    for (int i = 0; i < colSize; i++) {
      cols[i] = buf.readLong();
    }
  }

  @Override
  public int bufferLen() {
    return 4 + 4 + result.length * 8 + cols.length * 8;
  }
}
