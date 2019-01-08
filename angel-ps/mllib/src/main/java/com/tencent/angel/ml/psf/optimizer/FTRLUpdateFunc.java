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

public class FTRLUpdateFunc extends OptMMUpdateFunc {
    private static final Log LOG = LogFactory.getLog(FTRLUpdateFunc.class);

    public FTRLUpdateFunc() {
        super();
    }

    public FTRLUpdateFunc(int matId, int factor, double alpha, double beta, double lambda1, double lambda2, int epoch) {
        super(matId, new int[]{factor}, new double[]{alpha, beta, lambda1, lambda2, epoch, 1});
    }

    public FTRLUpdateFunc(int matId, int factor, double alpha, double beta, double lambda1, double lambda2, int epoch, int batchSize) {
        super(matId, new int[]{factor}, new double[]{alpha, beta, lambda1, lambda2, epoch, batchSize});
    }

    @Override
    public void update(ServerPartition partition, int factor, double[] scalars) {
        double alpha = scalars[0];
        double beta = scalars[1];
        double lambda1 = scalars[2];
        double lambda2 = scalars[3];
        int epoch = (int) scalars[4];
        int batchSize = (int) scalars[5];

        for (int f = 0; f < factor; f++) {
            ServerRow gradientServerRow = partition.getRow(f + 3 * factor);
            try {
                gradientServerRow.startWrite();
                Vector weight = partition.getRow(f).getSplit();
                Vector zModel = partition.getRow(f + factor).getSplit();
                Vector nModel = partition.getRow(f + 2 * factor).getSplit();
                Vector gradient = gradientServerRow.getSplit();

                if (batchSize > 1) {
                    gradient.idiv(batchSize);
                }

                Vector delta = OptFuncs.ftrldelta(nModel, gradient, alpha);
                Ufuncs.iaxpy2(nModel, gradient, 1);
                zModel.iadd(gradient.sub(delta.mul(weight)));

                Vector newWeight = Ufuncs.ftrlthreshold(zModel, nModel, alpha, beta, lambda1, lambda2);
                weight.setStorage(newWeight.getStorage());

                gradient.clear();
            } finally {
                gradientServerRow.endWrite();
            }
        }
    }
}
