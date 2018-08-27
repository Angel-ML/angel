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


package com.tencent.angel.ml.matrix.psf.update.enhance;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * `FullUpdateParam` is Parameter class for `FullUpdateFunc`
 */
public class FullUpdateParam extends UpdateParam {

  private final double[] values;

  public FullUpdateParam(int matrixId, double[] values) {
    super(matrixId, false);
    this.values = values;
  }

  @Override public List<PartitionUpdateParam> split() {
    List<PartitionKey> partList =
      PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    int size = partList.size();
    List<PartitionUpdateParam> partParams = new ArrayList<>(size);

    for (PartitionKey part : partList) {
      partParams.add(new FullPartitionUpdateParam(matrixId, part, values));
    }

    return partParams;
  }

  public static class FullPartitionUpdateParam extends PartitionUpdateParam {
    private double[] values;

    public FullPartitionUpdateParam(int matrixId, PartitionKey partKey, double[] values) {
      super(matrixId, partKey, false);
      this.values = values;
    }

    public FullPartitionUpdateParam() {
      super();
    }

    @Override public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(values.length);
      for (double value : values) {
        buf.writeDouble(value);
      }
    }

    @Override public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int size = buf.readInt();
      double[] values = new double[size];
      for (int i = 0; i < size; i++) {
        values[i] = buf.readDouble();
      }
      this.values = values;
    }

    @Override public int bufferLen() {
      return super.bufferLen() + 4 + 8 * this.values.length;
    }

    public double[] getValues() {
      return this.values;
    }

    @Override public String toString() {
      return "FullPartitionUpdateParam values size=" + this.values.length + ", toString()=" + super
        .toString() + "]";
    }

  }
}
