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

public class KmeansUpdateFunc extends OptMMUpdateFunc {

    private static final Log LOG = LogFactory.getLog(KmeansUpdateFunc.class);

    public KmeansUpdateFunc() {
        super();
    }

    public KmeansUpdateFunc(int matId, int factor) {
        this(matId, factor, 1);
    }

    public KmeansUpdateFunc(int matId, int factor, int batchSize) {
        super(matId, new int[]{factor}, new double[]{batchSize});
    }

    @Override
    void update(RowBasedPartition partition, int factor, double[] scalars) {
        double batchSize = scalars[0];

        for (int f = 0; f < factor; f++) {
            ServerRow gradientServerRow = partition.getRow(f + factor);
            try {
                gradientServerRow.startWrite();
                Vector weight = ServerRowUtils.getVector(partition.getRow(f));
                Vector gradient = ServerRowUtils.getVector(gradientServerRow);

                if (batchSize > 1) {
                    gradient.idiv(batchSize);
                }

                weight.iadd(gradient);

                gradient.clear();
            } finally {
                gradientServerRow.endWrite();
            }
        }
    }
}
