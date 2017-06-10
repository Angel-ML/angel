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
 * `MUpdaterParam` a parameter class for `MUpdaterFunc`
 */
public class MUpdaterParam extends UpdaterParam {

  public static class MPartitionUpdaterParam extends PartitionUpdaterParam {
    private int[] rowIds;

    public MPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, int[] rowIds) {
      super(matrixId, partKey, false);
      this.rowIds = rowIds;
    }

    public MPartitionUpdaterParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowIds.length);
      for (int rowId: rowIds){
        buf.writeInt(rowId);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int rowLength = buf.readInt();
      rowIds = new int[rowLength];
      for (int i = 0; i < rowLength; i++){
        rowIds[i] = buf.readInt();
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 4 + 4 * rowIds.length;
    }

    public int[] getRowIds() {
      return rowIds;
    }

    @Override
    public String toString() {
      return "MPartitionUpdaterParam [rowIds=" + Arrays.toString(rowIds) + ", toString()=" + super.toString() + "]";
    }
  }

  private final int[] rowIds;

  public MUpdaterParam(int matrixId, int[] rowIds) {
    super(matrixId, false);
    this.rowIds = rowIds;
  }

  @Override
  public List<PartitionUpdaterParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId);

    int size = partList.size();
    List<PartitionUpdaterParam> partParams = new ArrayList<PartitionUpdaterParam>(size);
    for (PartitionKey part : partList) {
      for (int rowId : rowIds) {
        if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
          throw new RuntimeException("Wrong rowId!");
        }
      }
      partParams.add(new MPartitionUpdaterParam(matrixId, part, rowIds));
    }

    return partParams;
  }

}
