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
package com.tencent.angel.graph.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AdjustParam extends UpdateParam {

  private byte[] dataBuf;
  private int bufLength;

  public AdjustParam(int matrixId,
                     int seed,
                     int negative,
                     int partitionId,
                     float[] gradient,
                     int[] src,
                     int[] dst) {
    super(matrixId);
    this.bufLength = 12 + src.length * 8 + gradient.length * 4;
    this.dataBuf = new byte[bufLength];
    ByteBuffer wrapper = ByteBuffer.wrap(this.dataBuf);
    wrapper.putInt(seed);
    wrapper.putInt(partitionId);
    wrapper.putInt(src.length);
    int inc = 0;
    for (int a = 0; a < src.length; a++) {
      wrapper.putInt(src[a]);
      wrapper.putInt(dst[a]);
      for (int b = 0; b < negative + 1; b++) {
        wrapper.putFloat(gradient[inc++]);
      }
    }
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager()
        .getPartitions(matrixId);
    List<PartitionUpdateParam> params = new ArrayList<>();
    for (PartitionKey pkey : pkeys) {
      AdjustPartitionParam partParam = new AdjustPartitionParam(matrixId, pkey, dataBuf, bufLength);
      params.add(partParam);
    }
    return params;
  }
}

