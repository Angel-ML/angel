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

package com.tencent.angel.ml.GBDT.psf;

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
public class HistAggrParam extends GetParam {

  public static class HistPartitionAggrParam extends PartitionGetParam {
    private int rowId;
    private int splitNum;
    private float minChildWeight;
    private float regAlpha;
    private float regLambda;

    public HistPartitionAggrParam(int matrixId, PartitionKey partKey, int rowId, int splitNum,
      float minChildWeight, float regAlpha, float regLambda) {
      super(matrixId, partKey);
      this.rowId = rowId;
      this.splitNum = splitNum;
      this.minChildWeight = minChildWeight;
      this.regAlpha = regAlpha;
      this.regLambda = regLambda;
    }

    public HistPartitionAggrParam() {
      this(0, null, 0, 0, 0.0f, 0.0f, 0.0f);
    }

    @Override public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowId);
      buf.writeInt(splitNum);
      buf.writeFloat(minChildWeight);
      buf.writeFloat(regAlpha);
      buf.writeFloat(regLambda);
    }

    @Override public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      rowId = buf.readInt();
      splitNum = buf.readInt();
      minChildWeight = buf.readFloat();
      regAlpha = buf.readFloat();
      regLambda = buf.readFloat();
    }

    @Override public int bufferLen() {
      return super.bufferLen() + 4 * 2 + 4 * 3;
    }

    public int getRowId() {
      return rowId;
    }

    public int getSplitNum() {
      return splitNum;
    }


    public float getMinChildWeight() {
      return minChildWeight;
    }

    public float getRegAlpha() {
      return regAlpha;
    }

    public float getRegLambda() {
      return regLambda;
    }
  }


  private final int rowId;
  private final int splitNum;
  private float minChildWeight;
  private float regAlpha;
  private float regLambda;

  public HistAggrParam(int matrixId, int rowId, int splitNum, float minChildWeight, float regAlpha,
    float regLambda) {

    super(matrixId);
    this.rowId = rowId;
    this.splitNum = splitNum;
    this.minChildWeight = minChildWeight;
    this.regAlpha = regAlpha;
    this.regLambda = regLambda;
  }

  @Override public List<PartitionGetParam> split() {
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (PartitionKey part : parts) {
      partParams.add(
        new HistPartitionAggrParam(matrixId, part, rowId, splitNum, minChildWeight, regAlpha,
          regLambda));
    }

    return partParams;
  }

}
