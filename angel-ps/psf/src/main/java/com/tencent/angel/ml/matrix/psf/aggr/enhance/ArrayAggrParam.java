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
 */

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.common.Utils;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * `FullAggrParam` is the parameter of `FullAggrFunc`
 */
public class ArrayAggrParam extends GetParam {

  public static class ArrayPartitionAggrParam extends PartitionGetParam {
    int rowId;
    long[] cols;

    public ArrayPartitionAggrParam(int matrixId, PartitionKey partKey, int rowId, long[] cols) {
      super(matrixId, partKey);
      this.rowId = rowId;
      this.cols = cols;
    }

    public ArrayPartitionAggrParam() {
      super();
    }

    public int getRowId() {
      return rowId;
    }

    public long[] getCols() {
      return cols;
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(cols.length);
      for (int i = 0; i < cols.length; i++) {
        buf.writeLong(cols[i]);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      this.rowId = buf.readInt();
      int size = buf.readInt();
      this.cols = new long[size];
      for (int i = 0; i < size; i++) {
        this.cols[i] = buf.readLong();
      }
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 4 + 4 + cols.length * 8;
    }
  }


  int rowId;
  long[] cols;
  public ArrayAggrParam(int matrixId, int rowId, long[] cols) {
    super(matrixId);
    this.rowId = rowId;

    Arrays.sort(cols);
    this.cols = cols;
  }



  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    List<PartitionGetParam> partParams = new ArrayList<>();

    int beginIdx = 0;
    int endIdx = 0;
    for (PartitionKey part: parts) {
      if (Utils.withinPart(part, new int[]{rowId})) {
        long startCol = part.getStartCol();
        long endCol = part.getEndCol();

        if (cols[beginIdx] >= startCol) {
          while (endIdx < cols.length && cols[endIdx] < endCol) endIdx++;

          long[] partCols = new long[endIdx - beginIdx];
          System.arraycopy(cols, beginIdx, partCols, 0, endIdx - beginIdx);
          partParams.add(new ArrayPartitionAggrParam(matrixId, part, rowId, partCols));

          if (endIdx == cols.length) break;
          beginIdx = endIdx;
        }
      }
    }

    return partParams;
  }

}
