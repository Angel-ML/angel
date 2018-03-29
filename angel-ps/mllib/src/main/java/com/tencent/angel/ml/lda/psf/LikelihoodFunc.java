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
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.impl.PSContext;
import com.tencent.angel.ps.impl.matrix.ServerDenseIntRow;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.special.Gamma;

import java.nio.IntBuffer;
import java.util.List;

public class LikelihoodFunc extends GetFunc {

  private static final Log LOG = LogFactory.getLog(LikelihoodFunc.class);

  public LikelihoodFunc(int matrixId, float beta) {
    super(new LikelihoodParam(matrixId, beta));
  }

  public LikelihoodFunc() {
    super(null);
  }

  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartitionKey pkey = partParam.getPartKey();

    pkey = psContext.getMatrixMetaManager().getMatrixMeta(pkey.getMatrixId())
      .getPartitionMeta(pkey.getPartitionId()).getPartitionKey();

    int ws = pkey.getStartRow();
    int es = pkey.getEndRow();

    if (partParam instanceof LikelihoodParam.LikelihoodPartParam) {
      LikelihoodParam.LikelihoodPartParam param = (LikelihoodParam.LikelihoodPartParam) partParam;
      float beta = param.getBeta();

      double lgammaBeta = Gamma.logGamma(beta);

      double ll = 0;
      for (int w = ws; w < es; w++) {
        ServerRow row = psContext.getMatrixStorageManager().getRow(pkey, w);
        ll += likelihood(row, beta, lgammaBeta);
      }

      return new ScalarPartitionAggrResult(ll);
    } else {
      return null;
    }
  }

  private double likelihood(ServerRow row, float beta, double lgammaBeta) {
    int len = (int) (row.getEndCol() - row.getStartCol());
    double ll = 0;
    if (row instanceof ServerDenseIntRow) {
      IntBuffer buf = ((ServerDenseIntRow) row).getData();
      for (int i = 0; i < len; i++) {
        if (buf.get(i) > 0)
          ll += Gamma.logGamma(buf.get(i) + beta) - lgammaBeta;
      }
    } else
      throw new AngelException("should be ServerDenseIntRow");
    return ll;
  }

  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    double ll = 0;
    for (PartitionGetResult r : partResults) {
      ll += ((ScalarPartitionAggrResult) r).result;
    }
    return new ScalarAggrResult(ll);
  }
}
