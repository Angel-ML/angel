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

import java.util.ArrayList;
import java.util.List;

import com.tencent.angel.ml.matrix.psf.updater.base.PartitionUpdaterParam;
import com.tencent.angel.ml.matrix.psf.updater.base.UpdaterParam;
import io.netty.buffer.ByteBuf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * `VAUpdaterParam` is Parameter class for `VAUpdateFunc`
 */
public class VAUpdaterParam extends UpdaterParam {

  public static class VAPartitionUpdaterParam extends PartitionUpdaterParam {
    private int rowId;
    private int start;
    private int end;
    private double[] array;
    private double[] arraySlice;

    public VAPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int rowId, int start, int end, double[] array) {
      super(matrixId, partKey, false);
      this.rowId = rowId;
      this.start = start;
      this.end = end;
      this.array = array;
    }

    public VAPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(end - start);
      for (int i = start; i < end; i++) {
        buf.writeDouble(array[i]);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      int length = buf.readInt();
      arraySlice = new double[length];
      for (int i = 0; i < length; i++) {
        arraySlice[i] = buf.readDouble();
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 8 + (end - start) * 8;
    }

    public int getRowId() {
      return rowId;
    }

    public double[] getArraySlice() {
      return arraySlice;
    }

    @Override
    public String toString() {
      return "VAPartitionUpdaterParam [rowId=" + rowId + ", toString()="
          + super.toString() + "]";
    }
  }

  private final int rowId;
  private final double[] array;


  public VAUpdaterParam(int matrixId, int rowId, double[] array) {
    super(matrixId, false);
    this.rowId = rowId;
    this.array = array;

  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId);

    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(new VAPartitionUpdaterParam(matrixId, part, rowId,
          part.getStartCol(), part.getEndCol(), array));
    }

    return partParams;
  }

}
