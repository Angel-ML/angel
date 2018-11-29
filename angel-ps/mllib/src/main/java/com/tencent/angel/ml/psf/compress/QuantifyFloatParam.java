package com.tencent.angel.ml.psf.compress;

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


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.core.utils.JCompressUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;


public class QuantifyFloatParam extends UpdateParam {

  private final int rowId;
  private final float[] array;
  private int numBits;

  public QuantifyFloatParam(int matrixId, int rowId, float[] array, int numBits) {
    super(matrixId, false);
    this.rowId = rowId;
    this.array = array;
    this.numBits = numBits;
  }

  @Override public List<PartitionUpdateParam> split() {
    List<PartitionKey> partList =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId, rowId);

    int size = partList.size();
    List<PartitionUpdateParam> partParams = new ArrayList<>(size);
    for (PartitionKey part : partList) {
      if (rowId < part.getStartRow() || rowId >= part.getEndRow()) {
        throw new RuntimeException("Wrong rowId!");
      }
      partParams.add(
          new QuantifyFloatPartParam(matrixId, part, rowId, (int) part.getStartCol(),
              (int) part.getEndCol(), array, numBits));
    }

    return partParams;
  }

  public static class QuantifyFloatPartParam extends PartitionUpdateParam {

    private int rowId;
    private int start;
    private int end;
    private float[] array;
    private float[] arraySlice;
    private int numBits;

    public QuantifyFloatPartParam(int matrixId, PartitionKey partKey,
        int rowId, int start, int end, float[] array, int numBits) {
      super(matrixId, partKey, false);
      this.rowId = rowId;
      this.start = start;
      this.end = end;
      this.array = array;
      this.numBits = numBits;
    }

    public QuantifyFloatPartParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      JCompressUtils.Quantification.serializeFloat(buf, array, start, end, numBits);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      arraySlice = JCompressUtils.Quantification.deserializeFloat(buf);
    }

    @Override
    public int bufferLen() {

      return super.bufferLen() + 20 + (int) Math.ceil((end - start) * numBits / 8);
    }

    public int getRowId() {
      return rowId;
    }

    public float[] getArraySlice() {
      return arraySlice;
    }

    @Override
    public String toString() {
      return "QuantifyDoublePartUParam [rowId=" + rowId + ", numBits=" + numBits
          + ", toString()=" + super.toString() + "]";
    }
  }
}

