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

package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * `MUpdateParam` a parameter class for `MUpdateFunc`
 */
public class MUpdateParam extends UpdateParam {

  public static class MPartitionUpdateParam extends PartitionUpdateParam {
    private int[] rowIds;

    public MPartitionUpdateParam(
        int matrixId, PartitionKey partKey, int[] rowIds) {
      super(matrixId, partKey, false);
      this.rowIds = rowIds;
    }

    public MPartitionUpdateParam() {
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
      return "MPartitionUpdateParam [rowIds=" + Arrays.toString(rowIds) + ", toString()=" + super.toString() + "]";
    }
  }

  private final int[] rowIds;

  public MUpdateParam(int matrixId, int[] rowIds) {
    super(matrixId, false);
    this.rowIds = rowIds;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixMetaManager()
        .getPartitions(matrixId);

    int size = partList.size();
    List<PartitionUpdateParam> partParams = new ArrayList<PartitionUpdateParam>(size);
    for (PartitionKey part : partList) {
      if (Utils.withinPart(part, rowIds)) {
        partParams.add(new MPartitionUpdateParam(matrixId, part, rowIds));
      }
    }
    if (partParams.isEmpty()) {
      System.out.println("Rows must in same partition.");
    }

    return partParams;
  }

}
