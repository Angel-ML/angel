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

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MomentumUpdateFunc extends OptMMUpdateFunc {

  private static final Log LOG = LogFactory.getLog(MomentumUpdateFunc.class);

  public MomentumUpdateFunc() {
    super();
  }

  public MomentumUpdateFunc(int matId, int factor, double momentum, double lr) {
    this(matId, factor, momentum, lr, 0.0, 1);
  }

  public MomentumUpdateFunc(int matId, int offset, double momentum, double lr, double regParam) {
    this(matId, offset, momentum, lr, regParam, 1);
  }

  public MomentumUpdateFunc(int matId, int offset, double momentum, double lr, double regParam,
      int batchSize) {
    super(matId, new int[]{offset}, new double[]{momentum, lr, regParam, batchSize});
  }

  @Override
  public void update(RowBasedPartition partition, int factor, double[] scalars) {
    double momentum = scalars[0];
    double lr = scalars[1];
    double regParam = scalars[2];
    double batchSize = scalars[3];

    for (int f = 0; f < factor; f++) {
      ServerRow gradientServerRow = partition.getRow(f + 2 * factor);
      try {
        gradientServerRow.startWrite();
        Vector weight = ServerRowUtils.getVector(partition.getRow(f));
        Vector velocity = ServerRowUtils.getVector(partition.getRow(f + factor));
        Vector gradient = ServerRowUtils.getVector(gradientServerRow);

        if (batchSize > 1) {
          gradient.idiv(batchSize);
        }

        if (regParam != 0.0) {
          gradient.iaxpy(weight, regParam);
        }

        velocity.imul(momentum).iadd(gradient);
        weight.isub(velocity.mul(lr));

        gradient.clear();
      } finally {
        gradientServerRow.endWrite();
      }
    }
  }

}
