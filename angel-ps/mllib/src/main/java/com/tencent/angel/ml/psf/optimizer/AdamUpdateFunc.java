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

public class AdamUpdateFunc extends OptMMUpdateFunc {
    private static final Log LOG = LogFactory.getLog(AdamUpdateFunc.class);

    public AdamUpdateFunc() {
        super();
    }

    public AdamUpdateFunc(int matId, int factor, double gamma, double epsilon, double beta, double lr,
                          double regParam, int iteration) {
        super(matId, new int[]{factor}, new double[]{gamma, epsilon, beta, lr, regParam, iteration, 1});
    }

    public AdamUpdateFunc(int matId, int factor, double gamma, double epsilon, double beta, double lr,
                          double regParam, int iteration, int batchSize) {
        super(matId, new int[]{factor}, new double[]{gamma, epsilon, beta, lr, regParam, iteration, batchSize});
    }

    @Override
    public void update(ServerPartition partition, int factor, double[] scalars) {
        double gamma = scalars[0];
        double epsilon = scalars[1];
        double beta = scalars[2];
        double lr = scalars[3];
        double regParam = scalars[4];
        double epoch = scalars[5];
        double batchSize = scalars[6];

        if (epoch == 0)
            epoch = 1;

        double powBeta = Math.pow(beta, epoch);
        double powGamma = Math.pow(gamma, epoch);

        for (int f = 0; f < factor; f++) {
            ServerRow gradientServerRow = partition.getRow(f + 3 * factor);
            try {
                gradientServerRow.startWrite();
                Vector weight = partition.getRow(f).getSplit();
                Vector velocity = partition.getRow(f + factor).getSplit();
                Vector square = partition.getRow(f + 2 * factor).getSplit();
                Vector gradient = gradientServerRow.getSplit();

                if (batchSize > 1)
                    gradient.idiv(batchSize);

                if (regParam != 0.0) {
                    gradient.iaxpy(weight, regParam);
                }

                OptFuncs.iexpsmoothing(velocity, gradient, beta);
                OptFuncs.iexpsmoothing2(square, gradient, gamma);

                Vector delta = OptFuncs.adamdelta(velocity, square, powBeta, powGamma);
                weight.iaxpy(delta, -lr);
                gradient.clear();
            } finally {
                gradientServerRow.endWrite();
            }
        }
    }

}
