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

import java.nio.DoubleBuffer;

/**
 * `ArrayPartitionAggrResult` is a result Class for all Aggregate function whose result is double[]
 */
public class SBPartitionAggrResult extends PartitionGetResult {
  private int[] rowIds = null;
  private double[][] data = null;

  public SBPartitionAggrResult(int[] rowId, double[][] data) {
    this.rowIds = rowId;
    this.data = data;
  }

  public SBPartitionAggrResult() {
  }

  public int[] getRowIds() {
    return rowIds;
  }

  public double[][] getData() {
    return data;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(rowIds.length);
    for (int i = 0; i < rowIds.length; i++) {
      buf.writeInt(rowIds[i]);
    }

    buf.writeInt(data.length);
    for (int i = 0; i < data.length; i++) {
      buf.writeInt(data[i].length);
      for (int j = 0; j < data[i].length; j++) {
        buf.writeDouble(data[i][j]);
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int size = buf.readInt();
    this.rowIds = new int[size];
    for (int i = 0; i < size; i++) {
      this.rowIds[i] = buf.readInt();
    }

    size = buf.readInt();
    this.data = new double[size][];
    for (int i = 0; i < size; i++) {
      int colSize = buf.readInt();
      double[] colData = new double[colSize];
      for (int j = 0; j < colSize; j++) {
        colData[j] = buf.readDouble();
      }
      this.data[i] = colData;
    }
  }

  @Override
  public int bufferLen() {
    int dataLen = 0;
    dataLen += 4;
    for (int i = 0; i < data.length; i++) {
      dataLen += 4 + data[i].length * 8;
    }

    return 4 + rowIds.length * 8 + dataLen;
  }
}
