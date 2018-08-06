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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * `FullPartitionAggrResult` is a result Class for all Aggregate function whose result is double[][]
 */
public class FullPartitionAggrResult extends PartitionGetResult {
  private double[][] result;
  private int[] partInfo;

  public FullPartitionAggrResult(double[][] result, int[] partInfo) {
    this.result = result;
    this.partInfo = partInfo;
  }

  public FullPartitionAggrResult() {}

  public double[][] getResult() {
    return result;
  }

  public int[] getPartInfo() {
    return partInfo;
  }

  @Override
  public void serialize(ByteBuf buf) {
    int rows = result.length;
    int cols = result[0].length;

    buf.writeInt(rows);
    buf.writeInt(cols);
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        buf.writeDouble(result[i][j]);
      }
    }

    buf.writeInt(partInfo.length);
    for (int i = 0; i < partInfo.length; i++) {
      buf.writeInt(partInfo[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    int rows = buf.readInt();
    int cols = buf.readInt();
    result = new double[rows][cols];
    for (int i = 0; i < rows; i++) {
      for (int j = 0; j < cols; j++) {
        result[i][j] = buf.readDouble();
      }
    }

    int infoSize = buf.readInt();
    partInfo = new int[infoSize];
    for (int i = 0; i < infoSize; i++) {
      partInfo[i] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    int rows = result.length;
    int cols = result[0].length;
    return (4 + 4 + (rows + cols) * 8) + (4 + partInfo.length * 4);
  }
}
