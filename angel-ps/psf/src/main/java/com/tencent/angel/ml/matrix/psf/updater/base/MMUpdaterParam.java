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

package com.tencent.angel.ml.matrix.psf.updater.base;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.updater.base.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.psf.updater.base.UpdaterParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * `MMUpdaterParam` is Parameter class for `MMUpdaterFunc`
 */
public class MMUpdaterParam extends UpdaterParam {

  public static class MMPartitionUpdaterParam extends PartitionUpdaterParam {
    private int[] rowIds;
    private double[] scalars;

    public MMPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int[] rowIds, double[] scalars) {
      super(matrixId, partKey, false);
      this.rowIds = rowIds;
      this.scalars = scalars;
    }

    public MMPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowIds.length);
      for (int rowId: rowIds) {
        buf.writeInt(rowId);
      }
      buf.writeInt(scalars.length);
      for (double scalar: scalars) {
        buf.writeDouble(scalar);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int rowLength = buf.readInt();
      this.rowIds = new int[rowLength];
      for (int i = 0; i < rowLength; i++) {
        this.rowIds[i] = buf.readInt();
      }

      int scalarLength = buf.readInt();
      this.scalars = new double[scalarLength];
      for (int i = 0; i < scalarLength; i++) {
        this.scalars[i] = buf.readDouble();
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + (4 + 4 * rowIds.length) + (4 + 8 * scalars.length);
    }

    public int[] getRowIds() {
      return rowIds;
    }

    public double[] getScalars() {
      return scalars;
    }

    @Override
    public String toString() {
      return "MMPartitionUpdaterParam [rowIds=" + Arrays.toString(rowIds) + ", scalars="
          + Arrays.toString(rowIds) +  ", toString()=" + super.toString() + "]";
    }
  }

  private final int[] rowIds;
  private final double[] scalars;

  public MMUpdaterParam(int matrixId, int[] rowIds, double[] scalars) {
    super(matrixId, false);
    this.rowIds = rowIds;
    this.scalars = scalars;
  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      for (int rowId : rowIds) {
        if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
          throw new RuntimeException("Wrong rowId!");
        }
      }
      partParams.add(new MMPartitionUpdaterParam(matrixId, part, rowIds, scalars));
    }

    return partParams;
  }

}
