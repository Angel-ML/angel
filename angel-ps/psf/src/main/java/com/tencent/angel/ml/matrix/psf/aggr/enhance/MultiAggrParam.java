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
 * `MultiAggrParam` is the parameter of `MultiAggrFunc`
 */
public class MultiAggrParam extends GetParam {

  public static class MultiPartitionAggrParam extends PartitionGetParam {
    private int[] rowIds;

    public MultiPartitionAggrParam(int matrixId, PartitionKey partKey, int[] rowIds) {
      super(matrixId, partKey);
      this.rowIds = rowIds;
    }

    public MultiPartitionAggrParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowIds.length);
      for (int i = 0; i < rowIds.length; i++) {
        buf.writeInt(rowIds[i]);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int rowLen = buf.readInt();
      rowIds = new int[rowLen];
      for (int i = 0; i < rowLen; i++) {
        rowIds[i] = buf.readInt();
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 8 * rowIds.length;
    }

    public int[] getRowIds() {
      return rowIds;
    }

  }

  private final int[] rowIds;

  public MultiAggrParam(int matrixId, int[] rowIds) {
    super(matrixId);
    this.rowIds = rowIds;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (PartitionKey part : parts) {
      partParams.add(new MultiPartitionAggrParam(matrixId, part, rowIds));
    }

    return partParams;
  }

}
