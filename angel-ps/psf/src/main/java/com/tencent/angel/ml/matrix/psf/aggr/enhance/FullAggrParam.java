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

package com.tencent.angel.ml.matrix.psf.aggr.enhance;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * `FullAggrParam` is the parameter of `FullAggrFunc`
 */
public class FullAggrParam extends GetParam {

  public static class FullPartitionAggrParam extends PartitionGetParam {

    public FullPartitionAggrParam(int matrixId, PartitionKey partKey) {
      super(matrixId, partKey);
    }

    public FullPartitionAggrParam() {
      super();
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
    }

    @Override
    public int bufferLen() {
      return super.bufferLen();
    }
  }

  public FullAggrParam(int matrixId) {
    super(matrixId);
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (PartitionKey part : parts) {
      partParams.add(new FullPartitionAggrParam(matrixId, part));
    }

    return partParams;
  }

}
