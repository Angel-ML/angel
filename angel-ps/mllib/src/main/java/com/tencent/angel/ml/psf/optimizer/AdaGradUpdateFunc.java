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

import com.tencent.angel.ml.math2.ufuncs.OptFuncs;
import com.tencent.angel.ml.math2.ufuncs.Ufuncs;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class AdaGradUpdateFunc extends OptMMUpdateFunc {

  private static final Log LOG = LogFactory.getLog(AdaGradUpdateFunc.class);

  public AdaGradUpdateFunc() {
    super();
  }

  public AdaGradUpdateFunc(int matId, int factor, double epsilon, double beta, double lr,
      double regL1Param, double regL2Param, int epoch) {
    super(matId, new int[]{factor},
        new double[]{epsilon, beta, lr, regL1Param, regL2Param, epoch, 1});
  }

  public AdaGradUpdateFunc(int matId, int factor, double epsilon, double beta, double lr,
      double regL1Param, double regL2Param, int epoch, int batchSize) {
    super(matId, new int[]{factor},
        new double[]{epsilon, beta, lr, regL1Param, regL2Param, epoch, batchSize});
  }

  @Override
  public void update(ServerPartition partition, int factor, double[] scalars) {
    double epsilon = scalars[0];
    double beta = scalars[1];
    double lr = scalars[2];
    double l1RegParam = scalars[3];
    double l2RegParam = scalars[4];
    double epoch = (int) scalars[5];
    double batchSize = (int) scalars[6];

    for (int f = 0; f < factor; f++) {
      ServerRow gradientServerRow = partition.getRow(f + 2 * factor);
      try {
        gradientServerRow.startWrite();
        Vector weight = partition.getRow(f).getSplit();
        Vector square = partition.getRow(f + factor).getSplit();
        Vector gradient = gradientServerRow.getSplit();

        if (batchSize > 1) {
          gradient.idiv(batchSize);
        }

        OptFuncs.iexpsmoothing2(square, gradient, beta);
        if (l2RegParam != 0) {
          gradient.iaxpy(weight, l2RegParam);
        }

        OptFuncs.iadagraddelta(gradient, square, l2RegParam, lr);
        weight.isub(gradient);

        if (l1RegParam != 0) {
          OptFuncs.iadagradthredshold(weight, square, l1RegParam, l2RegParam, lr);
        }

        gradient.clear();
      } finally {
        gradientServerRow.endWrite();
      }

    }
  }
}
