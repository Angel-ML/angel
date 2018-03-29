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

package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class LikelihoodParam extends GetParam {

  public static class LikelihoodPartParam extends PartitionGetParam {
    private float beta;

    public LikelihoodPartParam(int matrixId, PartitionKey pkey, float beta) {
      super(matrixId, pkey);
      this.beta = beta;
    }

    public LikelihoodPartParam() {
      super();
    }

    @Override public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeFloat(beta);
    }

    @Override public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      beta = buf.readFloat();
    }

    @Override public int bufferLen() {
      return super.bufferLen() + 4;
    }

    public float getBeta() {
      return beta;
    }
  }


  private final float beta;

  public LikelihoodParam(int matrixId, float beta) {
    super(matrixId);
    this.beta = beta;
  }

  @Override public List<PartitionGetParam> split() {
    List<PartitionGetParam> params = new ArrayList<>();
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    for (PartitionKey pkey : pkeys) {
      params.add(new LikelihoodPartParam(matrixId, pkey, beta));
    }

    return params;
  }
}
