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


package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarAggrResult;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.ScalarPartitionAggrResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerIntIntRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
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
        try {
          row.startRead();
          ll += likelihood((ServerIntIntRow) row, beta, lgammaBeta);
        } finally {
          row.endRead();
        }
      }

      return new ScalarPartitionAggrResult(ll);
    } else {
      throw new AngelException("Should be LikelihoodParam.LikelihoodPartParam");
    }
  }

  private double likelihood(ServerIntIntRow row, float beta, double lgammaBeta) {
    int len = (int) (row.getEndCol() - row.getStartCol());
    double ll = 0;
    if (row.isDense()) {
      int[] values = row.getValues();
      for (int i = 0; i < len; i++) {
        if (values[i] > 0)
          ll += Gamma.logGamma(values[i] + beta) - lgammaBeta;
      }
    } else if (row.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iterator = row.getIter();
      ;
      while (iterator.hasNext()) {
        Int2IntMap.Entry entry = iterator.next();
        int val = entry.getIntValue();
        if (val > 0)
          ll += Gamma.logGamma(val + beta) - lgammaBeta;
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
