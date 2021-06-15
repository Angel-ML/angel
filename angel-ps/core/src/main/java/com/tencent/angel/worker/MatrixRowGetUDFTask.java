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


package com.tencent.angel.worker;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.psf.get.indexed.LongIndexGet;
import com.tencent.angel.ml.matrix.psf.get.indexed.LongIndexGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.task.BaseTask;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class MatrixRowGetUDFTask extends BaseTask<Long, Long, Long> {
    private static final Log LOG = LogFactory.getLog(MatrixRowGetTask.class);

    public MatrixRowGetUDFTask(TaskContext taskContext) {
        super(taskContext);
    }

    @Override public void preProcess(TaskContext taskContext) {
    }

    @Override public Long parse(Long key, Long value) {
        return null;
    }

    @Override public void run(TaskContext taskContext) throws AngelException {
        try {
            MatrixClient matrixClient = taskContext.getMatrix(MatrixRowGetTest.SPARSE_LONGKEY_FLOAT_MAT);
            MatrixMeta matrixMeta =
                    PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(MatrixRowGetTest.SPARSE_LONGKEY_FLOAT_MAT);

            long[] indices = genLongIndexs(matrixMeta.getColNum(), MatrixRowGetTest.longnnz);
            long[] getIndices = genGetLongIndexs(indices, MatrixRowGetTest.nGet);


            LongFloatVector deltatest = VFactory.sparseLongKeyFloatVector( matrixMeta.getColNum(),MatrixRowGetTest.longnnz);
            for (int i = 0; i < MatrixRowGetTest.longnnz; i++) {
                deltatest.set(indices[i], (float) (indices[i] * 1.3));
            }
            deltatest.setRowId(0);

            LOG.info(" delta use " + deltatest.getType() + " type storage");

            int updateTime = 0;
            long startTs = System.currentTimeMillis();
            try {
                matrixClient.increment(0,deltatest,true);
                long startoneTs = System.currentTimeMillis();
                LOG.info("increment time= " + (startoneTs - startTs));
                while (true) {
                    LongIndexGet funca = new LongIndexGet(new LongIndexGetParam(matrixClient.getMatrixId(), 0, getIndices));
                    LongFloatVector tempOne = (LongFloatVector) ((GetRowResult) PSAgentContext.get().getUserRequestAdapter().get(funca).get()).getRow();
                    updateTime++;
                    if(updateTime % 10 == 0) {
                        LOG.info("get times = " + updateTime + ", avg update time=" + (System.currentTimeMillis() - startoneTs) / updateTime);
                    }
                }
            } catch (Throwable ie) {
                LOG.info("mistake happened in MatrixRowGet try");
            }
        } catch (InvalidParameterException e) {
            LOG.error("get matrix failed ", e);
            throw new AngelException(e);
        }
    }

    public static long[] genLongIndexs(long feaNum, int nnz) {
        long[] sortedIndex = new long[nnz];
        Random random = new Random(System.currentTimeMillis());
        for (int i = 1; i < nnz; i++) {
            sortedIndex[i] = Math.abs(random.nextLong()) % feaNum;
        }
        return sortedIndex;
    }

    public static long[] genGetLongIndexs(long[] indices, int nGet){
        if (indices.length < nGet) {
            return indices;
        }
        long[] getIndexs = new long[nGet];
        Random random = new Random(System.currentTimeMillis());
        int temp = 0;
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= nGet; i++) {
            temp = random.nextInt(indices.length);
            if (!(list.contains(temp))) {
                getIndexs[i - 1] = indices[temp];
                list.add(temp);
            } else {
                i--;
            }
        }
        return getIndexs;
    }
}
