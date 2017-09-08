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
import com.tencent.angel.common.Serialize;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * `MFUpdateParam` is Parameter class for `MFUpdateFunc`
 */
public class MFUpdateParam extends UpdateParam {

  private final int[] rowIds;
  private final Serialize func;

  public MFUpdateParam(int matrixId, int[] rowIds, Serialize func) {
    super(matrixId, false);
    this.rowIds = rowIds;
    this.func = func;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> partList = PSAgentContext.get()
        .getMatrixPartitionRouter()
        .getPartitionKeyList(matrixId);

    int size = partList.size();
    List<PartitionUpdateParam> partParams = new ArrayList<PartitionUpdateParam>(size);
    for (PartitionKey part : partList) {
      partParams.add(new MFPartitionUpdateParam(matrixId, part, rowIds, func));
    }

    return partParams;
  }

  public static class MFPartitionUpdateParam extends PartitionUpdateParam {
    private int[] rowIds;
    private Serialize func;

    public MFPartitionUpdateParam(int matrixId, PartitionKey partKey, int[] rowIds, Serialize func) {
      super(matrixId, partKey, false);
      this.rowIds = rowIds;
      this.func = func;
    }

    public MFPartitionUpdateParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowIds.length);
      for (int rowId : rowIds) {
        buf.writeInt(rowId);
      }

      byte[] bytes = func.getClass().getName().getBytes();
      buf.writeInt(bytes.length);
      buf.writeBytes(bytes);
      func.serialize(buf);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int rowIdNum = buf.readInt();
      int[] rowIds = new int[rowIdNum];
      for (int i = 0; i < rowIdNum; i++) {
        rowIds[i] = buf.readInt();
      }
      this.rowIds = rowIds;

      int size = buf.readInt();
      byte[] bytes = new byte[size];
      buf.readBytes(bytes);
      try {
        this.func = (Serialize) Class.forName(new String(bytes)).newInstance();
      } catch (ReflectiveOperationException e) {
        throw new RuntimeException(e);
      }
      this.func.deserialize(buf);
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + (4 + rowIds.length * 4)
          + (4 + func.getClass().getName().getBytes().length + func.bufferLen());
    }

    public int[] getRowIds() {
      return this.rowIds;
    }

    public Serialize getFunc() {
      return this.func;
    }

    @Override
    public String toString() {
      return "MFPartitionUpdateParam rowIds=" + Arrays.toString(rowIds) + ", func=" +
          func.getClass().getName() + ", toString()=" + super.toString() + "]";
    }

  }

}
