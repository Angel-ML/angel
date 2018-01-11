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
 * `UnaryAggrParam` is Parameter of `UnaryAggrFunc`.
 */
public class UnaryAggrParam extends GetParam {

  public static class UnaryPartitionAggrParam extends PartitionGetParam {
    private int rowId;

    public UnaryPartitionAggrParam(int matrixId, PartitionKey partKey, int rowId) {
      super(matrixId, partKey);
      this.rowId = rowId;
    }

    public UnaryPartitionAggrParam() {
      this(0, null, 0);
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 4;
    }

    public int getRowId() {
      return rowId;
    }

  }

  private final int rowId;

  public UnaryAggrParam(int matrixId, int rowId) {
    super(matrixId);
    this.rowId = rowId;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (PartitionKey part : parts) {
      partParams.add(new UnaryPartitionAggrParam(matrixId, part, rowId));
    }

    return partParams;
  }

}
