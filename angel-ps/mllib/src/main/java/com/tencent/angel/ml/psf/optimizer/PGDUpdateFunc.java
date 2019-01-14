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


package com.tencent.angel.ml.psf.optimizer;

import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PGDUpdateFunc extends OptMMUpdateFunc {

  private static final Log LOG = LogFactory.getLog(PGDUpdateFunc.class);

  public PGDUpdateFunc() {
    super();
  }

  public PGDUpdateFunc(int matId, int factor, double lr, double l1RegParam, double l2RegParam) {
    super(matId, new int[]{factor}, new double[]{lr, l1RegParam, l2RegParam, 1});
  }

  public PGDUpdateFunc(int matId, int factor, double lr, double l1RegParam, double l2RegParam,
      int batchSize) {
    super(matId, new int[]{factor}, new double[]{lr, l1RegParam, l2RegParam, batchSize});
  }

  @Override
  public void update(ServerPartition partition, int factor, double[] scalars) {
    double lr = scalars[0];
    double l1RegParam = scalars[1];
    double l2RegParam = scalars[2];
    double batchSize = (int) scalars[3];

    for (int f = 0; f < factor; f++) {
      ServerRow gradientServerRow = partition.getRow(f + factor);
      try {
        gradientServerRow.startWrite();
        Vector weight = partition.getRow(f).getSplit();
        Vector gradient = gradientServerRow.getSplit();

        if (batchSize > 1) {
          gradient.idiv(batchSize);
        }

        double lrTemp = lr / (1 + l2RegParam * lr);
        if (l2RegParam != 0.0) {
          weight.imul(1 - lrTemp * l2RegParam).iaxpy(gradient, -lrTemp);
        } else {
          weight.iaxpy(gradient, -lrTemp);
        }

        if (l1RegParam != 0) {
          Ufuncs.isoftthreshold(weight, lrTemp * l1RegParam);
        }

        gradient.clear();
      } finally {
        gradientServerRow.endWrite();
      }

    }
  }

}
