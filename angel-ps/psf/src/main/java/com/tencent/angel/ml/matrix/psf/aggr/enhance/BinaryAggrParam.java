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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * `BinaryAggrParam` is the parameter of `BinaryAggrFunc`
 */
public class BinaryAggrParam extends GetParam {

  public static class BinaryPartitionAggrParam extends PartitionGetParam {
    private int rowId1;
    private int rowId2;

    public BinaryPartitionAggrParam(int matrixId, PartitionKey partKey, int rowId1, int rowId2) {
      super(matrixId, partKey);
      this.rowId1 = rowId1;
      this.rowId2 = rowId2;
    }

    public BinaryPartitionAggrParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId1);
      buf.writeInt(rowId2);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId1 = buf.readInt();
      rowId2 = buf.readInt();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 8;
    }

    public int getRowId1() {
      return rowId1;
    }

    public int getRowId2() {
      return rowId2;
    }
  }

  private final int rowId1;
  private final int rowId2;

  public BinaryAggrParam(int matrixId, int rowId1, int rowId2) {
    super(matrixId);
    this.rowId1 = rowId1;
    this.rowId2 = rowId2;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (PartitionKey part : parts) {
      partParams.add(new BinaryPartitionAggrParam(matrixId, part, rowId1, rowId2));
    }

    return partParams;
  }

}
