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

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.MMUpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;


public abstract class OptMMUpdateFunc extends UpdateFunc {
    public OptMMUpdateFunc() {
        super(null);
    }

    public OptMMUpdateFunc(int matrixId, int[] rowIds, double[] scalars) {
        super(new MMUpdateParam(matrixId, rowIds, scalars));
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        ServerPartition part = psContext.getMatrixStorageManager()
                .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

        assert part != null;
        MMUpdateParam.MMPartitionUpdateParam vs2 = (MMUpdateParam.MMPartitionUpdateParam) partParam;

        update(part, vs2.getRowIds()[0], vs2.getScalars());
    }

    abstract void update(ServerPartition partition, int factor, double[] scalars);
}
